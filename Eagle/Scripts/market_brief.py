#!/usr/bin/env python3
"""
market_brief.py — Eagle orchestrator: gather, analyze, brief.

Reads live ticks, runs the analysis scripts, gathers intelligence,
and produces a full briefing ready to paste into an AI.

Usage:
    cd ~/Documents/Projets/TCoding/Eagle
    python3 Scripts/market_brief.py              # NQ (default)
    python3 Scripts/market_brief.py GC            # GC
    python3 Scripts/market_brief.py NQ --skip     # skip analysis, briefing only
    python3 Scripts/market_brief.py NQ --save     # save to Reports/
"""
import sys
import os
import subprocess
import argparse
from datetime import datetime, timedelta
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("pip3 install polars")
    sys.exit(1)

# ═════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = BASE_DIR / "Scripts"
DATA_DIR = BASE_DIR / "Data"
CANDLES_DIR = DATA_DIR / "Candles"
CSV_HISTORY = DATA_DIR / "CSV_History"
PROFILE_CSV = DATA_DIR / "Reports" / "Market_Profile" / "daily_profile.csv"
NAKED_CSV = DATA_DIR / "Reports" / "Market_Profile" / "naked_poc.csv"
REGIMES_CSV = DATA_DIR / "Reports" / "Order_Flow" / "orderflow_regimes.csv"
REPORTS_DIR = DATA_DIR / "Reports" / "Briefings"

TARGETS = {
    "NQ": {"T1": 25, "T2": 50, "T3": 100},
    "GC": {"T1": 5, "T2": 12, "T3": 25},
}

TICK_SIZE = {"GC": 0.10, "NQ": 0.25}

IB_HOUR = {"GC": 13, "NQ": 14}

RTH = {
    "GC": {"start": 13, "end": 22},
    "NQ": {"start": 13, "end": 21},
}


# ═════════════════════════════════════════════════════════════════════════════
# STEP 1: READ LIVE TICKS
# ═════════════════════════════════════════════════════════════════════════════

def read_live_ticks(symbol):
    """Read the listener's live tick CSV. Return last price + stats."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    filepath = CSV_HISTORY / f"{symbol}_live_{today}.csv"

    if not filepath.exists():
        # Try yesterday
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        filepath = CSV_HISTORY / f"{symbol}_live_{yesterday}.csv"

    if not filepath.exists():
        return None

    df = pl.read_csv(filepath)
    if df.shape[0] == 0:
        return None

    last_row = df.tail(1)
    last_price = last_row["close"][0]
    raw_time = str(last_row["time_utc"][0])
    # Keep only HH:MM UTC
    last_time_str = raw_time[11:16] + " UTC" if len(raw_time) > 16 else raw_time

    # Session stats
    total_vol = df["volume"].sum()
    total_delta = (df["ask_vol"].sum() - df["bid_vol"].sum())
    n_ticks = df.shape[0]

    return {
        "price": last_price,
        "time": last_time_str,
        "volume": total_vol,
        "delta": total_delta,
        "ticks": n_ticks,
        "source": str(filepath.name),
    }


# ═════════════════════════════════════════════════════════════════════════════
# STEP 2: RUN ANALYSIS SCRIPTS
# ═════════════════════════════════════════════════════════════════════════════

def run_script(name, args=None, silent=True):
    """Run an Eagle script and return the exit code."""
    script = SCRIPTS_DIR / name
    if not script.exists():
        print(f"  ⚠ {name} not found")
        return False

    cmd = [sys.executable, str(script)]
    if args:
        cmd.extend(args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=silent,
            text=True,
            timeout=300,
            cwd=str(BASE_DIR),
        )
        if result.returncode != 0:
            print(f"  ⚠ {name} error (code {result.returncode})")
            if silent and result.stderr:
                # Show the last 3 error lines
                err_lines = result.stderr.strip().split("\n")
                for line in err_lines[-3:]:
                    print(f"    {line}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print(f"  ⚠ {name} timeout (5 min)")
        return False


def run_analysis():
    """Run the full analysis chain."""
    steps = [
        ("market_profile.py", None, "TPO Profile"),
        ("orderflow_regimes.py", None, "Order Flow"),
    ]

    for script_name, args, label in steps:
        print(f"  [{label}]...", end=" ", flush=True)
        ok = run_script(script_name, args)
        print("OK" if ok else "FAIL")


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3: GATHER INTELLIGENCE
# ═════════════════════════════════════════════════════════════════════════════

def load_profile(symbol, n_days=10):
    if not PROFILE_CSV.exists():
        return None
    df = pl.read_csv(PROFILE_CSV)
    return df.filter(pl.col("symbol") == symbol).sort("date", descending=True).head(n_days)


def load_full_profile(symbol):
    if not PROFILE_CSV.exists():
        return None
    df = pl.read_csv(PROFILE_CSV)
    return df.filter(pl.col("symbol") == symbol).sort("date")


def load_naked_pocs(symbol, price_ref, radius=300):
    if not NAKED_CSV.exists():
        return []
    df = pl.read_csv(NAKED_CSV)
    naked = df.filter(
        (pl.col("symbol") == symbol) & (pl.col("filled") == False)
    ).sort("poc")

    nearby = []
    for row in naked.iter_rows(named=True):
        poc = row["poc"]
        dist = poc - price_ref
        if abs(dist) <= radius:
            nearby.append({
                "date": row["origin_date"],
                "poc": poc,
                "type": row["day_type"],
                "age": row["days_alive"],
                "dist": dist,
            })
    nearby.sort(key=lambda x: abs(x["dist"]))
    return nearby


def load_regimes(symbol, n_bars=36):
    if not REGIMES_CSV.exists():
        return None
    df = pl.read_csv(REGIMES_CSV)
    return df.filter(pl.col("symbol") == symbol).sort("datetime_utc", descending=True).head(n_bars)


def get_candle_price(symbol):
    """Last price + time from 5m candles (fallback if no live ticks)."""
    path = CANDLES_DIR / f"{symbol}_5m.parquet"
    if not path.exists():
        return None, None
    df = pl.read_parquet(path)
    last = df.sort("datetime_utc").tail(1)
    if last.shape[0] == 0:
        return None, None
    return last["close"][0], last["datetime_utc"][0]


def get_today_candles(symbol):
    path = CANDLES_DIR / f"{symbol}_5m.parquet"
    if not path.exists():
        return None
    df = pl.read_parquet(path)
    today = datetime.utcnow().date()
    return df.filter(pl.col("datetime_utc").dt.date() == today)


def compute_ib(candles_today, symbol):
    if candles_today is None or candles_today.shape[0] == 0:
        return None
    ib_hour = IB_HOUR.get(symbol, 14)
    ib_candles = candles_today.filter(pl.col("datetime_utc").dt.hour() == ib_hour)
    if ib_candles.shape[0] == 0:
        return None
    return {
        "high": ib_candles["high"].max(),
        "low": ib_candles["low"].min(),
        "range": ib_candles["high"].max() - ib_candles["low"].min(),
        "volume": ib_candles["volume"].sum(),
    }


# ═════════════════════════════════════════════════════════════════════════════
# ANALYSIS
# ═════════════════════════════════════════════════════════════════════════════

def compute_trends(full_profile, symbol):
    """Weekly + Daily trend."""
    if full_profile is None or full_profile.shape[0] < 10:
        return None

    # --- DAILY (5 days) ---
    daily = full_profile.tail(5)
    d_closes = daily["day_close"].to_list()
    d_opens = daily["day_open"].to_list()
    d_highs = daily["day_high"].to_list()
    d_lows = daily["day_low"].to_list()
    d_dates = daily["date"].to_list()

    d_change = d_closes[-1] - d_opens[0]
    hh = sum(1 for i in range(1, len(d_highs)) if d_highs[i] > d_highs[i - 1])
    ll = sum(1 for i in range(1, len(d_lows)) if d_lows[i] < d_lows[i - 1])
    up_days = sum(1 for i in range(1, len(d_closes)) if d_closes[i] > d_closes[i - 1])
    dn_days = sum(1 for i in range(1, len(d_closes)) if d_closes[i] < d_closes[i - 1])

    if d_change > 0:
        d_dir, d_str = "BULLISH", min(3, up_days)
    elif d_change < 0:
        d_dir, d_str = "BEARISH", min(3, dn_days)
    else:
        d_dir, d_str = "NEUTRAL", 0

    # --- WEEKLY (4 complete weeks) ---
    df_w = full_profile.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("dt")
    ).with_columns(
        pl.col("dt").dt.iso_year().alias("iso_year"),
        pl.col("dt").dt.week().alias("iso_week"),
    )

    weeks = df_w.group_by(["iso_year", "iso_week"]).agg([
        pl.col("day_open").first().alias("w_open"),
        pl.col("day_high").max().alias("w_high"),
        pl.col("day_low").min().alias("w_low"),
        pl.col("day_close").last().alias("w_close"),
        pl.col("dt").min().alias("w_start"),
        pl.len().alias("w_days"),
    ]).sort("w_start")

    last_w = weeks.tail(1)
    if last_w["w_days"][0] < 3:
        complete = weeks.head(weeks.shape[0] - 1)
    else:
        complete = weeks

    r4 = complete.tail(4)
    w_closes = r4["w_close"].to_list()
    w_opens = r4["w_open"].to_list()
    w_change = w_closes[-1] - w_opens[0]
    w_up = sum(1 for i in range(len(w_closes)) if w_closes[i] > w_opens[i])
    w_dn = sum(1 for i in range(len(w_closes)) if w_closes[i] < w_opens[i])

    if w_change > 0:
        w_dir, w_str = "BULLISH", min(3, w_up)
    elif w_change < 0:
        w_dir, w_str = "BEARISH", min(3, w_dn)
    else:
        w_dir, w_str = "NEUTRAL", 0

    bar = lambda s: "●" * s + "○" * (3 - s)

    return {
        "daily": {"dir": d_dir, "bar": bar(d_str), "chg": d_change, "hh": hh, "ll": ll,
                  "from": d_dates[0], "to": d_dates[-1]},
        "weekly": {"dir": w_dir, "bar": bar(w_str), "chg": w_change, "up": w_up, "dn": w_dn},
        "confluence": d_dir if d_dir == w_dir else (
            "DIVERGENCE" if d_dir != "NEUTRAL" and w_dir != "NEUTRAL" else "UNDECIDED"),
    }


def analyze_context(profile, price_ref, symbol):
    if profile is None or profile.shape[0] == 0:
        return None

    last = profile.row(0, named=True)
    prev = profile.row(1, named=True) if profile.shape[0] > 1 else None
    targets = TARGETS[symbol]

    poc, vah, val = last["poc"], last["vah"], last["val"]

    if price_ref > vah:
        position = "ABOVE VA"
    elif price_ref < val:
        position = "BELOW VA"
    elif price_ref > poc:
        position = "POC-VAH"
    else:
        position = "VAL-POC"

    recent_5 = profile.head(5)
    types = recent_5["day_type"].to_list()
    ranges = recent_5["day_range"].to_list()

    return {
        "date": last["date"], "poc": poc, "vah": vah, "val": val,
        "va_width": last.get("va_width", vah - val),
        "type": last["day_type"], "range": last["day_range"],
        "ib_range": last["ib_range"], "direction": last.get("direction", "?"),
        "position": position,
        "types_5d": types, "avg_range_5d": sum(ranges) / len(ranges) if ranges else 0,
        "prev": prev,
    }


def find_zones(ctx, naked_pocs, ib, price_ref, symbol):
    targets = TARGETS[symbol]
    zones = []
    poc, vah, val = ctx["poc"], ctx["vah"], ctx["val"]

    # VAH Rejection
    if abs(price_ref - vah) < targets["T1"]:
        zones.append({
            "signal": "SELL", "name": "VAH Rejection",
            "entry": f"~{vah:.2f}", "stop": f"{vah + targets['T1']:.2f}",
            "T1": f"{vah - targets['T1']:.2f}", "T2": f"{poc:.2f} (POC)", "T3": f"{val:.2f} (VAL)",
            "condition": "Confirmed rejection with Exhaustion or Absorption at VAH",
        })

    # VAL Rejection
    if abs(price_ref - val) < targets["T1"]:
        zones.append({
            "signal": "BUY", "name": "VAL Rejection",
            "entry": f"~{val:.2f}", "stop": f"{val - targets['T1']:.2f}",
            "T1": f"{val + targets['T1']:.2f}", "T2": f"{poc:.2f} (POC)", "T3": f"{vah:.2f} (VAH)",
            "condition": "Confirmed rejection with Exhaustion or Absorption at VAL",
        })

    # IB Breakout
    if ib:
        if price_ref > ib["high"]:
            zones.append({
                "signal": "BUY", "name": "IB Breakout Long",
                "entry": f">{ib['high']:.2f}", "stop": f"{ib['high'] - targets['T1'] * 0.5:.2f}",
                "T1": f"{ib['high'] + targets['T1']:.2f}", "T2": f"{ib['high'] + targets['T2']:.2f}",
                "T3": f"Trail", "condition": "Breakout with Aggression + volume",
            })
        if price_ref < ib["low"]:
            zones.append({
                "signal": "SELL", "name": "IB Breakout Short",
                "entry": f"<{ib['low']:.2f}", "stop": f"{ib['low'] + targets['T1'] * 0.5:.2f}",
                "T1": f"{ib['low'] - targets['T1']:.2f}", "T2": f"{ib['low'] - targets['T2']:.2f}",
                "T3": f"Trail", "condition": "Breakout with Aggression + volume",
            })

    # Naked POC Magnet
    for npoc in naked_pocs[:3]:
        dist = npoc["dist"]
        if abs(dist) < targets["T1"] or abs(dist) > targets["T3"] * 2:
            continue
        direction = "BUY" if dist > 0 else "SELL"
        zones.append({
            "signal": direction, "name": f"Naked POC ({npoc['date']})",
            "entry": f"@{price_ref:.2f}",
            "stop": f"{price_ref + (targets['T1'] * (-1 if direction == 'BUY' else 1)):.2f}",
            "T1": f"{price_ref + (dist * 0.4):.2f}", "T2": f"{npoc['poc']:.2f}",
            "T3": "Beyond",
            "condition": f"POC {npoc['poc']:.2f} ({abs(dist):.0f}pts, {npoc['age']}d, {npoc['type']})",
        })

    return zones


def analyze_regimes(regimes_df):
    if regimes_df is None or regimes_df.shape[0] == 0:
        return None
    counts = regimes_df.group_by("regime").agg(pl.len().alias("n")).sort("n", descending=True)
    dom = counts.row(0, named=True)
    last_6 = regimes_df.head(6)["regime"].to_list()
    return {"dominant": dom["regime"], "count": dom["n"], "total": regimes_df.shape[0], "last_30m": last_6}


# ═════════════════════════════════════════════════════════════════════════════
# FORMAT BRIEFING
# ═════════════════════════════════════════════════════════════════════════════

def format_briefing(symbol, price_ref, price_time, ctx, trends, ib, naked_pocs, zones, regimes, today_candles, live_stats):
    targets = TARGETS[symbol]
    lines = []
    sep = "─" * 60

    # ── PRICE ──
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    lines.append(f"  {symbol}@{price_ref:.2f}  {price_time}  —  {now_str}")

    # ── TREND ──
    if trends:
        w, d = trends["weekly"], trends["daily"]
        lines.append(f"\n{sep}")
        lines.append(f"  TREND")
        lines.append(sep)
        lines.append(f"  W: {w['dir']} {w['bar']}  {w['chg']:+.0f}pts 4w  ({w['up']}W↑ {w['dn']}W↓)")
        lines.append(f"  D: {d['dir']} {d['bar']}  {d['chg']:+.0f}pts 5d  (HH:{d['hh']} LL:{d['ll']})")
        lines.append(f"  → {trends['confluence']}")

    # ── POSITION ──
    lines.append(f"\n{sep}")
    lines.append(f"  POSITION: {ctx['position']}  (vs {ctx['date']})")
    lines.append(sep)
    lines.append(f"  POC {ctx['poc']:.2f}  |  VAH {ctx['vah']:.2f}  |  VAL {ctx['val']:.2f}  |  VA {ctx['va_width']:.0f}pts")
    lines.append(f"  Type: {ctx['type']}  Range: {ctx['range']:.0f}  IB: {ctx['ib_range']:.0f}  Dir: {ctx['direction']}")
    if ctx["prev"]:
        p = ctx["prev"]
        lines.append(f"  D-2: POC {p['poc']:.2f}  VAH {p['vah']:.2f}  VAL {p['val']:.2f}  ({p['day_type']})")

    # ── TODAY'S IB ──
    if ib:
        lines.append(f"\n{sep}")
        lines.append(f"  IB  H:{ib['high']:.2f}  L:{ib['low']:.2f}  R:{ib['range']:.0f}pts  V:{ib['volume']:,}")
        if ib["range"] > (150 if symbol == "NQ" else 16):
            lines.append(f"  ⚠ Wide IB — limited range day")
        elif ib["range"] < (80 if symbol == "NQ" else 8):
            lines.append(f"  ⚡ Narrow IB — breakout potential")

    # ── NAKED POCs ──
    if naked_pocs:
        lines.append(f"\n{sep}")
        lines.append(f"  NAKED POCs")
        lines.append(sep)
        for np in naked_pocs[:6]:
            arrow = "↑" if np["dist"] > 0 else "↓"
            lines.append(f"  {np['poc']:>10.2f}  {arrow}{abs(np['dist']):>5.0f}pts  ({np['date']}, {np['age']}d, {np['type']})")

    # ── ORDER FLOW ──
    if regimes:
        lines.append(f"\n{sep}")
        lines.append(f"  ORDER FLOW  {regimes['dominant']} ({regimes['count']}/{regimes['total']})")
        lines.append(f"  30min: {' → '.join(regimes['last_30m'])}")

    # ── SESSION ──
    if today_candles is not None and today_candles.shape[0] > 0:
        dh = today_candles["high"].max()
        dl = today_candles["low"].min()
        dv = today_candles["volume"].sum()
        dd = today_candles["ask_vol"].sum() - today_candles["bid_vol"].sum()
        lines.append(f"\n{sep}")
        lines.append(f"  SESSION  {dl:.2f}—{dh:.2f}  R:{dh - dl:.0f}pts  V:{dv:,}  Δ:{dd:+,}  ({today_candles.shape[0]}×5m)")

    # ── LIVE TICKS ──
    if live_stats:
        lines.append(f"  LIVE  {live_stats['ticks']:,} ticks  V:{live_stats['volume']:,}  Δ:{live_stats['delta']:+,}  ({live_stats['source']})")

    # ── 5D CONTEXT ──
    lines.append(f"\n{sep}")
    lines.append(f"  5D  Types: {', '.join(ctx['types_5d'])}  Avg range: {ctx['avg_range_5d']:.0f}pts")

    # ── ZONES / SIGNALS ──
    lines.append(f"\n{sep}")
    lines.append(f"  SIGNALS")
    lines.append(sep)
    if not zones:
        lines.append(f"  No signal — wait for setup")
    else:
        for i, z in enumerate(zones, 1):
            lines.append(f"  [{i}] {z['signal']} {z['name']}")
            lines.append(f"      E:{z['entry']}  Stop:{z['stop']}  T1:{z['T1']}  T2:{z['T2']}  T3:{z['T3']}")
            lines.append(f"      If: {z['condition']}")

    # ── TARGETS ──
    lines.append(f"\n{sep}")
    t = targets
    lines.append(f"  TARGETS  T1={t['T1']}  T2={t['T2']}  T3={t['T3']}pts")
    lines.append(f"  Buy  T1:{price_ref + t['T1']:.2f}  T2:{price_ref + t['T2']:.2f}  T3:{price_ref + t['T3']:.2f}")
    lines.append(f"  Sell T1:{price_ref - t['T1']:.2f}  T2:{price_ref - t['T2']:.2f}  T3:{price_ref - t['T3']:.2f}")

    lines.append("")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Eagle Market Brief")
    parser.add_argument("symbol", nargs="?", default="NQ", choices=["GC", "NQ"])
    parser.add_argument("--skip", action="store_true", help="Skip analysis, briefing only")
    parser.add_argument("--save", action="store_true", help="Save to Reports/Briefings/")
    args = parser.parse_args()

    symbol = args.symbol.upper()

    # ── STEP 1: Price from live ticks ──
    live = read_live_ticks(symbol)
    if live:
        price_ref = live["price"]
        price_time = live["time"]
    else:
        price_ref, price_dt = get_candle_price(symbol)
        if price_ref is None:
            print(f"  No price for {symbol}")
            sys.exit(1)
        price_time = price_dt.strftime("%H:%M") + " UTC" if price_dt else "?"
        live = None

    # ── STEP 2: Analysis (unless --skip) ──
    if not args.skip:
        print(f"  Analyzing {symbol}...")
        run_analysis()
        print()

    # ── STEP 3: Gather ──
    profile = load_profile(symbol, n_days=10)
    full_profile = load_full_profile(symbol)
    naked_pocs = load_naked_pocs(symbol, price_ref, radius=TARGETS[symbol]["T3"] * 3)
    regimes = load_regimes(symbol, n_bars=36)
    today_candles = get_today_candles(symbol)
    ib = compute_ib(today_candles, symbol)

    ctx = analyze_context(profile, price_ref, symbol)
    if not ctx:
        print(f"  No profile data for {symbol}")
        sys.exit(1)

    trends = compute_trends(full_profile, symbol)
    zones = find_zones(ctx, naked_pocs, ib, price_ref, symbol)
    regime_info = analyze_regimes(regimes)

    # ── STEP 4: Briefing ──
    briefing = format_briefing(symbol, price_ref, price_time, ctx, trends, ib,
                               naked_pocs, zones, regime_info, today_candles, live)
    print(briefing)

    if args.save:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"brief_{symbol}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.txt"
        out_path = REPORTS_DIR / filename
        with open(out_path, "w") as f:
            f.write(briefing)
        print(f"  → {out_path}")


if __name__ == "__main__":
    main()
