#!/usr/bin/env python3
"""
eagle_server.py — Eagle server: historical .scid + live UDP → WebSocket Dashboard
Architecture: Tornado (HTTP + WebSocket) — zero heavy dependency
"""
import os
import sys
import struct
import json
import socket
import threading
import asyncio
import calendar
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

import csv as csvmod

import tornado.ioloop
import tornado.web
import tornado.websocket

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Paths — adapt to your setup
BASE_DIR = Path(__file__).parent
SC_LIVE_DIR = Path("/Volumes/[C] Windows 11/SierraChart/Data")  # Sierra Chart live SCID
PULSE_SCID_DIR = BASE_DIR.parent / "Pulse" / "Data" / "Scid_Data"  # Pulse working copy (last/front/next, kept fresh by Pulse `sync`)
TC_ARCHIVE_DIR = Path("/Volumes/Sam128/TC_Sam128")    # TC deep archive (all 20 contracts)
EAGLE_DATA_DIR = BASE_DIR / "Data" / "CSV_History"     # Historical CSV
RECORD_DIR = BASE_DIR / "Data" / "CSV_History"          # Tick recording
CANDLES_DIR = BASE_DIR / "Data" / "Candles"             # Parquet candles (build_candles.py)
PROFILE_CSV = BASE_DIR / "Data" / "Reports" / "Market_Profile" / "daily_profile.csv"
STATIC_DIR = BASE_DIR / "static"

# Web server
HTTP_PORT = 8888

# UDP Live (Sierra Chart → Mac)
UDP_HOST = "0.0.0.0"
UDP_PORT = 11099

# Candles
DEFAULT_TIMEFRAME = 1  # minutes

# Volume Profile (Dalton)
VALUE_AREA_PCT = 0.70  # 70% of total volume
TICK_SIZE = {"GC": 0.10, "NQ": 0.25}  # Profile granularity

# SCID Format
SCID_HEADER_SIZE = 56
SCID_RECORD_SIZE = 40
SCID_RECORD_FMT = "<qffffIIII"
EXCEL_EPOCH = datetime(1899, 12, 30)

# Active contracts (the last in the list is the front month)
CONTRACT_CHAINS = {
    "GC": ["GCJ26-COMEX.scid"],
    "NQ": ["NQH26-CME.scid", "NQM26-CME.scid"],
}

# For compatibility — the front month (last in the chain)
ACTIVE_CONTRACTS = {sym: chain[-1] for sym, chain in CONTRACT_CHAINS.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# CSV HISTORY READER (from convert_to_eagle.py)
# ═══════════════════════════════════════════════════════════════════════════════

def read_csv_ticks(filepath, max_records=None):
    """Read an Eagle historical CSV and return a list of tick dicts."""
    import csv
    ticks = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if max_records and len(rows) > max_records:
        rows = rows[-max_records:]

    for row in rows:
        # Support both old format (DateTime_UTC, Open, High...) and new (time_utc, open, high...)
        dt_str = row.get("DateTime_UTC") or row.get("datetime_utc") or row.get("time_utc", "")
        dt_str = dt_str.strip()
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        ticks.append({
            "dt": dt,
            "open": float(row.get("Open") or row.get("open", 0)),
            "high": float(row.get("High") or row.get("high", 0)),
            "low": float(row.get("Low") or row.get("low", 0)),
            "close": float(row.get("Close") or row.get("Last") or row.get("close", 0)),
            "num_trades": int(float(row.get("NumTrades") or row.get("num_trades", 1))),
            "volume": int(float(row.get("Volume") or row.get("volume", 0))),
            "bid_vol": int(float(row.get("BidVolume") or row.get("bid_vol", 0))),
            "ask_vol": int(float(row.get("AskVolume") or row.get("ask_vol", 0))),
        })
    return ticks


def find_history_csv(symbol):
    """Search for the historical CSV for a symbol in Data/."""
    csv_path = EAGLE_DATA_DIR / f"{symbol}_history.csv"
    if csv_path.exists():
        return csv_path
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# SCID READER
# ═══════════════════════════════════════════════════════════════════════════════

def find_scid_file(filename):
    """Search for the .scid file: Sierra Chart live > Pulse working copy > TC archive."""
    for d in [SC_LIVE_DIR, PULSE_SCID_DIR, TC_ARCHIVE_DIR]:
        p = d / filename
        if p.exists():
            return p
    return None


def read_scid_ticks(filepath, max_records=None):
    """Read a .scid file and return a list of tick dicts."""
    file_size = os.path.getsize(filepath)
    total_records = (file_size - SCID_HEADER_SIZE) // SCID_RECORD_SIZE

    if total_records <= 0:
        return []

    # If max_records, read only the last N
    start_record = 0
    num_to_read = total_records
    if max_records and max_records < total_records:
        start_record = total_records - max_records
        num_to_read = max_records

    ticks = []
    with open(filepath, "rb") as f:
        f.seek(SCID_HEADER_SIZE + start_record * SCID_RECORD_SIZE)
        data = f.read(num_to_read * SCID_RECORD_SIZE)

    for rec in struct.iter_unpack(SCID_RECORD_FMT, data):
        dt = EXCEL_EPOCH + timedelta(microseconds=rec[0])
        ticks.append({
            "dt": dt,
            "open": rec[1],
            "high": rec[2],
            "low": rec[3],
            "close": rec[4],
            "num_trades": rec[5],
            "volume": rec[6],
            "bid_vol": rec[7],
            "ask_vol": rec[8],
        })

    return ticks


# ═══════════════════════════════════════════════════════════════════════════════
# CANDLE BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate_candles(ticks, timeframe_min=5):
    """Aggregate ticks into OHLCV candles."""
    if not ticks:
        return []

    candles = []
    current = None
    tf_delta = timedelta(minutes=timeframe_min)

    for t in ticks:
        # Round to the timeframe bucket
        dt = t["dt"]
        bucket_min = (dt.minute // timeframe_min) * timeframe_min
        bucket = dt.replace(minute=bucket_min, second=0, microsecond=0)

        if current is None or bucket > current["time"]:
            if current is not None:
                candles.append(current)
            current = {
                "time": bucket,
                "open": t["close"],  # The tick's close is the Last price
                "high": t["high"],
                "low": t["low"],
                "close": t["close"],
                "volume": t["volume"],
                "bid_vol": t["bid_vol"],
                "ask_vol": t["ask_vol"],
            }
        else:
            current["high"] = max(current["high"], t["high"])
            current["low"] = min(current["low"], t["low"])
            current["close"] = t["close"]
            current["volume"] += t["volume"]
            current["bid_vol"] += t["bid_vol"]
            current["ask_vol"] += t["ask_vol"]

    if current:
        candles.append(current)

    return candles


# ═══════════════════════════════════════════════════════════════════════════════
# VOLUME PROFILE & DALTON (VAH, VAL, POC)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_volume_profile(ticks, tick_size=0.10):
    """Compute the volume profile and Dalton levels for a session."""
    if not ticks:
        return {"poc": 0, "vah": 0, "val": 0, "profile": {}}

    # Build the profile: volume per price level rounded to tick_size
    profile = defaultdict(float)
    for t in ticks:
        # Use close (Last) as reference price
        price_level = round(round(t["close"] / tick_size) * tick_size, 2)
        profile[price_level] += t["volume"]

    if not profile:
        return {"poc": 0, "vah": 0, "val": 0, "profile": {}}

    # POC = price with the largest volume
    poc = max(profile, key=profile.get)
    total_vol = sum(profile.values())
    target_vol = total_vol * VALUE_AREA_PCT

    # Value Area: symmetric expansion from POC
    sorted_prices = sorted(profile.keys())
    poc_idx = sorted_prices.index(poc)

    va_vol = profile[poc]
    low_idx = poc_idx
    high_idx = poc_idx

    while va_vol < target_vol and (low_idx > 0 or high_idx < len(sorted_prices) - 1):
        # Compare the volume above and below
        vol_above = profile[sorted_prices[high_idx + 1]] if high_idx < len(sorted_prices) - 1 else 0
        vol_below = profile[sorted_prices[low_idx - 1]] if low_idx > 0 else 0

        if vol_above >= vol_below and high_idx < len(sorted_prices) - 1:
            high_idx += 1
            va_vol += profile[sorted_prices[high_idx]]
        elif low_idx > 0:
            low_idx -= 1
            va_vol += profile[sorted_prices[low_idx]]
        else:
            high_idx += 1
            va_vol += profile[sorted_prices[high_idx]]

    vah = sorted_prices[high_idx]
    val = sorted_prices[low_idx]

    return {
        "poc": poc,
        "vah": vah,
        "val": val,
        "profile": dict(profile),
    }


def get_session_ticks(ticks, session_date=None):
    """Filter ticks for one RTH (Regular Trading Hours) session.
    GC RTH: 08:20 - 13:30 ET | NQ RTH: 09:30 - 16:00 ET
    Simplified: take the full day for now."""
    if session_date is None and ticks:
        session_date = ticks[-1]["dt"].date()

    return [t for t in ticks if t["dt"].date() == session_date]


# ═══════════════════════════════════════════════════════════════════════════════
# DATA STORE (In memory)
# ═══════════════════════════════════════════════════════════════════════════════

class EagleData:
    """In-memory store for the server."""

    def __init__(self):
        self.candles = {}       # {"GC": [...], "NQ": [...]}
        self.dalton = {}        # {"GC": {"poc":..., "vah":..., "val":...}}
        self.live_candle = {}   # Candle being built (live)
        self.ws_clients = set()

    def load_historical(self, symbol, timeframe_min=DEFAULT_TIMEFRAME):
        """Load candles from Parquet (build_candles.py) + POC/VAH/VAL from daily_profile.csv."""

        # 1. Load 1m candles from Parquet
        parquet_path = CANDLES_DIR / f"{symbol}_1m.parquet"
        if parquet_path.exists():
            try:
                import polars as pl
                print(f"  [{symbol}] Loading {parquet_path.name}...", end=" ", flush=True)
                df = pl.read_parquet(parquet_path)

                # Fast column-wise conversion (no iter_rows)
                times = df["datetime_utc"].to_list()
                opens = df["open"].to_list()
                highs = df["high"].to_list()
                lows = df["low"].to_list()
                closes = df["close"].to_list()
                volumes = df["volume"].to_list()
                bid_vols = df["bid_vol"].to_list() if "bid_vol" in df.columns else [0] * len(times)
                ask_vols = df["ask_vol"].to_list() if "ask_vol" in df.columns else [0] * len(times)

                candles = []
                for i in range(len(times)):
                    candles.append({
                        "time": times[i],
                        "open": opens[i],
                        "high": highs[i],
                        "low": lows[i],
                        "close": closes[i],
                        "volume": volumes[i],
                        "bid_vol": bid_vols[i],
                        "ask_vol": ask_vols[i],
                    })
                self.candles[symbol] = candles
                print(f"{len(candles):,} candles")
            except Exception as e:
                print(f"  [{symbol}] Parquet error: {e}")
                return
        else:
            print(f"  [{symbol}] No 1m Parquet — run build_candles.py first")
            return

        # 2. Load POC/VAH/VAL from daily_profile.csv (last day)
        self.load_dalton_from_profile(symbol)

    def load_dalton_from_profile(self, symbol):
        """Load POC/VAH/VAL levels for the last day from daily_profile.csv."""
        if not PROFILE_CSV.exists():
            print(f"  [{symbol}] daily_profile.csv not found — no Dalton")
            return

        try:
            import polars as pl
            df = pl.read_csv(PROFILE_CSV)
            sym_df = df.filter(pl.col("symbol") == symbol).sort("date", descending=True)
            if sym_df.shape[0] == 0:
                print(f"  [{symbol}] No row in daily_profile.csv")
                return

            last = sym_df.row(0, named=True)
            poc = last.get("poc", 0)
            vah = last.get("vah", 0)
            val = last.get("val", 0)

            if poc and vah and val:
                self.dalton[symbol] = {"poc": poc, "vah": vah, "val": val, "profile": {}}
                print(f"  [{symbol}] Dalton ({last.get('date', '?')}): POC={poc:.2f} VAH={vah:.2f} VAL={val:.2f}")
            else:
                print(f"  [{symbol}] Empty POC/VAH/VAL in daily_profile.csv")
        except Exception as e:
            print(f"  [{symbol}] Dalton load error: {e}")

    def candles_to_json(self, symbol):
        """Format candles for TradingView Lightweight Charts."""
        if symbol not in self.candles:
            return "[]"
        result = []
        for c in self.candles[symbol]:
            result.append({
                "time": int(c["time"].timestamp()),
                "open": round(c["open"], 2),
                "high": round(c["high"], 2),
                "low": round(c["low"], 2),
                "close": round(c["close"], 2),
                "volume": c["volume"],
                "delta": c["ask_vol"] - c["bid_vol"],
            })
        return json.dumps(result)

    def dalton_to_json(self, symbol):
        """Format Dalton levels for the dashboard."""
        if symbol not in self.dalton:
            return "{}"
        d = self.dalton[symbol]
        return json.dumps({
            "poc": round(d["poc"], 2),
            "vah": round(d["vah"], 2),
            "val": round(d["val"], 2),
        })

    def add_live_tick(self, symbol, ts_str, price, volume, bid_vol, ask_vol):
        """Build server-side live candles."""
        try:
            dt = datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return

        tf = DEFAULT_TIMEFRAME
        bucket_min = (dt.minute // tf) * tf
        bucket = dt.replace(minute=bucket_min, second=0, microsecond=0)

        if symbol not in self.candles:
            self.candles[symbol] = []

        # New candle or update
        if (symbol not in self.live_candle or
                self.live_candle[symbol] is None or
                bucket > self.live_candle[symbol]["time"]):
            # Close the previous candle
            if symbol in self.live_candle and self.live_candle[symbol] is not None:
                self.candles[symbol].append(self.live_candle[symbol])
            # New candle
            self.live_candle[symbol] = {
                "time": bucket,
                "open": price, "high": price, "low": price, "close": price,
                "volume": volume, "bid_vol": bid_vol, "ask_vol": ask_vol,
            }
        else:
            c = self.live_candle[symbol]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            c["volume"] += volume
            c["bid_vol"] += bid_vol
            c["ask_vol"] += ask_vol

    def candles_plus_live_json(self, symbol, timeframe_min=1):
        """Return closed candles + current candle, at the requested timeframe."""
        all_candles = list(self.candles.get(symbol, []))
        if symbol in self.live_candle and self.live_candle[symbol] is not None:
            all_candles.append(self.live_candle[symbol])

        # Re-aggregate if the requested timeframe is greater than 1min
        if timeframe_min > 1 and all_candles:
            merged = []
            current = None
            for c in all_candles:
                bucket_min = (c["time"].minute // timeframe_min) * timeframe_min
                bucket = c["time"].replace(minute=bucket_min, second=0, microsecond=0)
                if current is None or bucket > current["time"]:
                    if current is not None:
                        merged.append(current)
                    current = {
                        "time": bucket,
                        "open": c["open"], "high": c["high"],
                        "low": c["low"], "close": c["close"],
                        "volume": c["volume"],
                        "bid_vol": c["bid_vol"], "ask_vol": c["ask_vol"],
                    }
                else:
                    current["high"] = max(current["high"], c["high"])
                    current["low"] = min(current["low"], c["low"])
                    current["close"] = c["close"]
                    current["volume"] += c["volume"]
                    current["bid_vol"] += c["bid_vol"]
                    current["ask_vol"] += c["ask_vol"]
            if current:
                merged.append(current)
            all_candles = merged

        result = []
        for c in all_candles:
            result.append({
                "time": calendar.timegm(c["time"].timetuple()),  # UTC
                "open": round(c["open"], 2),
                "high": round(c["high"], 2),
                "low": round(c["low"], 2),
                "close": round(c["close"], 2),
                "volume": c["volume"],
                "delta": c["ask_vol"] - c["bid_vol"],
            })
        return json.dumps(result)

    async def broadcast(self, message):
        """Send a message to all connected WebSocket clients."""
        dead = set()
        for ws in self.ws_clients:
            try:
                ws.write_message(message)
            except Exception:
                dead.add(ws)
        self.ws_clients -= dead


# Global instance
eagle = EagleData()


# ═══════════════════════════════════════════════════════════════════════════════
# UDP LIVE LISTENER (separate thread)
# ═══════════════════════════════════════════════════════════════════════════════

CSV_HEADER = ["time_utc", "open", "high", "low", "close", "volume", "num_trades", "bid_vol", "ask_vol", "delta"]


def get_csv_writer(symbol, csv_files={}):
    """Return a CSV writer for the symbol, one file per day.
    Append mode — auto-reopens if the day changes."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"{symbol}_{today}"

    if key not in csv_files:
        # Close the old file if the day changed
        old_key = f"{symbol}_{csv_files.get(f'{symbol}_date', '')}"
        if old_key in csv_files:
            csv_files[old_key]["file"].close()
            del csv_files[old_key]

        RECORD_DIR.mkdir(parents=True, exist_ok=True)
        filepath = RECORD_DIR / f"{symbol}_live_{today}.csv"
        is_new = not filepath.exists()
        f = open(filepath, "a", newline="")
        writer = csvmod.writer(f)
        if is_new:
            writer.writerow(CSV_HEADER)
        csv_files[key] = {"file": f, "writer": writer}
        csv_files[f"{symbol}_date"] = today
        print(f"  Recording → {filepath}")

    return csv_files[key]["writer"], csv_files[key]["file"]


def udp_listener(io_loop):
    """Listen to Sierra Chart UDP stream in a separate thread."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, UDP_PORT))
    sock.settimeout(1.0)

    print(f"  UDP listener active on :{UDP_PORT}")
    print(f"  Recording ticks → {RECORD_DIR}/")

    last_tick = {}
    tick_count = 0
    flush_counter = 0

    while True:
        try:
            data, _ = sock.recvfrom(1024)
            msg = data.decode("utf-8").strip()
        except socket.timeout:
            continue
        except Exception:
            continue

        parts = msg.split(",")
        if len(parts) != 6:
            continue

        symbol_raw, ts_str, price_s, vol_s, bid_s, ask_s = parts

        # Dedup filter
        tick_id = ts_str + price_s
        sym = symbol_raw.split("-")[0][:2]
        if tick_id == last_tick.get(sym):
            continue
        last_tick[sym] = tick_id

        try:
            price = float(price_s)
            volume = float(vol_s)
            bid_vol = float(bid_s)
            ask_vol = float(ask_s)
        except ValueError:
            continue

        # Log
        tick_count += 1
        if tick_count <= 5 or tick_count % 100 == 0:
            print(f"  TICK #{tick_count}: {sym} {price_s} vol={vol_s} [{ts_str}]")

        # Build the server-side candle
        eagle.add_live_tick(sym, ts_str, price, volume, bid_vol, ask_vol)

        # Build the WebSocket message
        ws_msg = json.dumps({
            "type": "tick",
            "symbol": sym,
            "time": ts_str,
            "price": price,
            "volume": volume,
            "bid_vol": bid_vol,
            "ask_vol": ask_vol,
        })

        # Broadcast via Tornado's IO loop (thread-safe)
        io_loop.add_callback(eagle.broadcast, ws_msg)

        # Record the tick to CSV
        delta = int(ask_vol - bid_vol)
        writer, fh = get_csv_writer(sym)
        writer.writerow([ts_str, price, price, price, price, int(volume), 1, int(bid_vol), int(ask_vol), delta])
        flush_counter += 1
        if flush_counter % 50 == 0:
            fh.flush()


# ═══════════════════════════════════════════════════════════════════════════════
# TORNADO HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.redirect("/static/dashboard.html")


class CandlesHandler(tornado.web.RequestHandler):
    def get(self, symbol):
        tf = int(self.get_argument("tf", "1"))
        self.set_header("Content-Type", "application/json")
        self.write(eagle.candles_plus_live_json(symbol.upper(), timeframe_min=tf))


class DaltonHandler(tornado.web.RequestHandler):
    def get(self, symbol):
        self.set_header("Content-Type", "application/json")
        self.write(eagle.dalton_to_json(symbol.upper()))


class LiveWSHandler(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True  # Accept all origins (local)

    def open(self):
        eagle.ws_clients.add(self)
        print(f"  WebSocket connected ({len(eagle.ws_clients)} clients)")

    def on_close(self):
        eagle.ws_clients.discard(self)
        print(f"  WebSocket disconnected ({len(eagle.ws_clients)} clients)")

    def on_message(self, message):
        pass  # Client does not send anything for now


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/api/candles/(\w+)", CandlesHandler),
        (r"/api/dalton/(\w+)", DaltonHandler),
        (r"/ws", LiveWSHandler),
        (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": str(STATIC_DIR)}),
    ])


def main():
    print("=" * 60)
    print("  EAGLE — Trading Dashboard")
    print("=" * 60)

    # 1. Load historical data (CSV or SCID if available)
    live_only = "--live" in sys.argv
    if live_only:
        print("\n[1] Pure live mode — skip historical")
    else:
        print("\n[1] Searching for historical data...")
        for sym in ACTIVE_CONTRACTS:
            eagle.load_historical(sym)
        if not any(eagle.candles.values()):
            print("  No history — pure live mode")

    # 2. Prepare the web server
    app = make_app()
    app.listen(HTTP_PORT)
    main_loop = tornado.ioloop.IOLoop.current()

    # 3. Start the UDP listener (separate thread, with ref to the main IOLoop)
    print("\n[2] Starting UDP listener...")
    udp_thread = threading.Thread(target=udp_listener, args=(main_loop,), daemon=True)
    udp_thread.start()

    # 4. Run
    print(f"\n[3] Web server started on http://localhost:{HTTP_PORT}")
    print(f"    Dashboard : http://localhost:{HTTP_PORT}/static/dashboard.html")
    print(f"    Candles API: http://localhost:{HTTP_PORT}/api/candles/GC")
    print(f"    Dalton API : http://localhost:{HTTP_PORT}/api/dalton/GC")
    print(f"\n    Ctrl+C to stop.\n")

    main_loop.start()


if __name__ == "__main__":
    main()
