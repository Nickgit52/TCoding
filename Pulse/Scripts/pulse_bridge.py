#!/usr/bin/env python3
"""
pulse_bridge.py — Read .scid + live CSV → 30-day Parquet (Front only)

Usage:
    python3 Scripts/pulse_bridge.py

Source  : Data/Scid_Data/*.scid + Data/Live_Data/*.csv + Data/contracts.json
Produces: Data/Flux_Data/GC_Tick_Flux.parquet (Front contract, 30d)
          Data/Flux_Data/NQ_Tick_Flux.parquet (Front contract, 30d)

Behavior:
    - Reads contracts.json to know Front / Next
    - Reads the .scid files present in Scid_Data
    - Loads live ticks from *_Live.csv
    - Merges → deduplicates → sorts
    - Compares Front vs Next volume (rollover detection)
    - Filters Front contract only
    - Trims to a rolling 30 days
    - Updates contracts.json with the rollover status
"""

import json
import os
import struct
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("Install polars: pip3 install polars")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR  = Path(__file__).parent.parent
DATA_DIR  = BASE_DIR / "Data"
SCID_DIR  = DATA_DIR / "Scid_Data"
FLUX_DIR  = DATA_DIR / "Flux_Data"
LIVE_DIR  = DATA_DIR / "Live_Data"
FLUX_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = {"GC": "COMEX", "NQ": "CME"}

KEEP_DAYS = 40

# Rollover calendar (for auto-flip if Next has overtaken Front by volume)
MONTH_CODES = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
               7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}
CONTRACT_CYCLES = {
    "GC": [2, 4, 6, 8, 10, 12],   # bimonthly COMEX
    "NQ": [3, 6, 9, 12],           # quarterly CME
}


def next_contract_after(symbol: str, contract: str) -> str:
    """
    Return the next contract in the calendar (e.g., GCM26 → GCQ26).
    Used to recompute Next after an auto-flip.
    """
    cycle = CONTRACT_CYCLES[symbol]
    code = contract[len(symbol)]       # e.g., "M" in "GCM26"
    year = int(contract[len(symbol) + 1:])  # e.g., 26

    # Reverse MONTH_CODES: "M" → 6
    code_to_month = {v: k for k, v in MONTH_CODES.items()}
    month = code_to_month[code]

    # Index in the cycle + move to the next
    idx = cycle.index(month)
    if idx + 1 < len(cycle):
        new_month = cycle[idx + 1]
        new_year = year
    else:
        new_month = cycle[0]
        new_year = year + 1

    return f"{symbol}{MONTH_CODES[new_month]}{new_year:02d}"

# SCID format
HEADER_SIZE  = 56
RECORD_SIZE  = 40
RECORD_FMT   = "<qffffIIII"
EXCEL_EPOCH  = datetime(1899, 12, 30)
CHUNK_SIZE   = 500_000

# Parquet schema
SCHEMA = {
    "datetime_utc" : pl.Datetime("us"),
    "contract"     : pl.Utf8,
    "open"         : pl.Float32,
    "high"         : pl.Float32,
    "low"          : pl.Float32,
    "close"        : pl.Float32,
    "volume"       : pl.Int32,
    "bid_vol"      : pl.Int32,
    "ask_vol"      : pl.Int32,
    "delta"        : pl.Int32,
}

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# CONTRACTS.JSON
# ═══════════════════════════════════════════════════════════════════════════════

def load_contracts() -> dict:
    """Load contracts.json. Returns {} if missing."""
    p = DATA_DIR / "contracts.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}


def save_contracts(data: dict):
    """Save contracts.json."""
    p = DATA_DIR / "contracts.json"
    with open(p, "w") as f:
        json.dump(data, f, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# SCID READER
# ═══════════════════════════════════════════════════════════════════════════════

def read_scid(filepath: Path) -> pl.DataFrame:
    """Read a full .scid → Polars DataFrame."""
    fsize    = os.path.getsize(filepath)
    nrec     = (fsize - HEADER_SIZE) // RECORD_SIZE
    contract = filepath.stem.split("-")[0].upper()

    timestamps = []
    opens      = []
    highs      = []
    lows       = []
    closes     = []
    volumes    = []
    bid_vols   = []
    ask_vols   = []

    with open(filepath, "rb") as f:
        f.seek(HEADER_SIZE)
        for start in range(0, nrec, CHUNK_SIZE):
            n    = min(CHUNK_SIZE, nrec - start)
            data = f.read(n * RECORD_SIZE)
            for rec in struct.iter_unpack(RECORD_FMT, data):
                dt = EXCEL_EPOCH + timedelta(microseconds=rec[0])
                timestamps.append(dt)
                opens.append(rec[1])
                highs.append(rec[2])
                lows.append(rec[3])
                closes.append(rec[4])
                volumes.append(rec[6])
                bid_vols.append(rec[7])
                ask_vols.append(rec[8])

    df = pl.DataFrame({
        "datetime_utc" : timestamps,
        "contract"     : [contract] * len(timestamps),
        "open"         : opens,
        "high"         : highs,
        "low"          : lows,
        "close"        : closes,
        "volume"       : volumes,
        "bid_vol"      : bid_vols,
        "ask_vol"      : ask_vols,
    }).with_columns([
        pl.col("open").cast(pl.Float32),
        pl.col("high").cast(pl.Float32),
        pl.col("low").cast(pl.Float32),
        pl.col("close").cast(pl.Float32),
        pl.col("volume").cast(pl.Int32),
        pl.col("bid_vol").cast(pl.Int32),
        pl.col("ask_vol").cast(pl.Int32),
        (pl.col("ask_vol").cast(pl.Int32) - pl.col("bid_vol").cast(pl.Int32)).alias("delta"),
    ])

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def find_scid_files(symbol: str) -> list[Path]:
    """Find all .scid files for a symbol in Scid_Data/."""
    exchange = SYMBOLS[symbol]
    pattern = f"{symbol}*-{exchange}.scid"
    return sorted(SCID_DIR.glob(pattern))


def parquet_path(symbol: str) -> Path:
    return FLUX_DIR / f"{symbol}_Tick_Flux.parquet"


def load_live(symbol: str) -> pl.DataFrame:
    """Load Live.csv (recent ticks from pulse_listen.py)."""
    p = LIVE_DIR / f"{symbol}_Live.csv"
    if not p.exists() or p.stat().st_size == 0:
        return pl.DataFrame(schema=SCHEMA)
    try:
        df = pl.read_csv(p, try_parse_dates=True)
        df = df.with_columns([
            pl.col("datetime_utc").cast(pl.Datetime("us")),
            pl.col("open").cast(pl.Float32),
            pl.col("high").cast(pl.Float32),
            pl.col("low").cast(pl.Float32),
            pl.col("close").cast(pl.Float32),
            pl.col("volume").cast(pl.Int32),
            pl.col("bid_vol").cast(pl.Int32),
            pl.col("ask_vol").cast(pl.Int32),
            pl.col("delta").cast(pl.Int32),
        ])
        return df
    except Exception:
        return pl.DataFrame(schema=SCHEMA)


def print_depth(symbol: str, df: pl.DataFrame):
    if df.is_empty():
        print(f"  {YELLOW}[{symbol}] Empty{RESET}")
        return
    n      = df.shape[0]
    oldest = df["datetime_utc"][0]
    newest = df["datetime_utc"][-1]
    days   = (newest - oldest).days
    size   = parquet_path(symbol).stat().st_size / 1e6
    print(
        f"  {GREEN}[{symbol}]{RESET} {n:,} ticks — "
        f"{oldest.strftime('%Y-%m-%d')} → {newest.strftime('%Y-%m-%d')} "
        f"({days}d) — {size:.1f} MB"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ROLLOVER DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def detect_rollover(all_data: pl.DataFrame, front: str, next_c: str) -> dict:
    """
    Compare daily Front vs Next volume over the last few days.
    Returns a dict with the rollover status.
    """
    if all_data.is_empty():
        return {"status": "unknown", "front_vol": 0, "next_vol": 0, "ratio": 0.0}

    # Last 3 full days
    latest = all_data["datetime_utc"].max()
    cutoff = latest - timedelta(days=3)
    recent = all_data.filter(pl.col("datetime_utc") >= cutoff)

    # Volume per contract on the most recent day
    last_day = latest - timedelta(days=1)
    yesterday = recent.filter(pl.col("datetime_utc") >= last_day)

    front_vol = yesterday.filter(
        pl.col("contract") == front
    )["volume"].sum() if not yesterday.filter(pl.col("contract") == front).is_empty() else 0

    next_vol = yesterday.filter(
        pl.col("contract") == next_c
    )["volume"].sum() if not yesterday.filter(pl.col("contract") == next_c).is_empty() else 0

    # Next/Front ratio
    if front_vol > 0:
        ratio = next_vol / front_vol
    elif next_vol > 0:
        ratio = 1.0  # Front is dead, Next has all the volume
    else:
        ratio = 0.0

    # Status
    if ratio >= 0.8:
        status = "IMMINENT"
    elif ratio >= 0.3:
        status = "APPROACHING"
    elif next_vol > 0:
        status = "EARLY"
    else:
        status = "STABLE"

    return {
        "status": status,
        "front_vol": int(front_vol),
        "next_vol": int(next_vol),
        "ratio": round(ratio, 3),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# BRIDGE
# ═══════════════════════════════════════════════════════════════════════════════

def bridge_symbol(symbol: str, contracts: dict) -> dict:
    """Bridge a symbol. Returns the rollover info."""
    front = contracts.get("front", "")
    next_c = contracts.get("next", "")

    print(f"\n  {CYAN}{'─' * 50}{RESET}")
    print(f"  {BOLD}{symbol}{RESET}  Front: {front}  Next: {next_c}")
    print(f"  {'─' * 50}")

    # 1. Read the .scid files
    scid_files = find_scid_files(symbol)
    if not scid_files:
        print(f"  {RED}No .scid found for {symbol}{RESET}")
        return {}

    scid_frames = []
    for p in scid_files:
        fsize = os.path.getsize(p)
        nrec  = (fsize - HEADER_SIZE) // RECORD_SIZE
        print(f"  {CYAN}[READ]{RESET} {p.name} — {nrec:,} ticks ({fsize/1e6:.0f} MB)...", end=" ", flush=True)
        df = read_scid(p)
        scid_frames.append(df)
        print("OK")

    all_scid = pl.concat(scid_frames)
    print(f"  Combined .scid: {all_scid.shape[0]:,} ticks")

    # 2. Load live
    live = load_live(symbol)
    if not live.is_empty():
        print(f"  Live: {live.shape[0]:,} ticks")
    else:
        print(f"  Live: no live ticks")

    # 3. Merge everything (all contracts)
    if live.is_empty():
        all_data = all_scid
    else:
        all_data = pl.concat([all_scid, live])

    all_data = (
        all_data
        .unique(subset=["datetime_utc", "close"], keep="first")
        .sort("datetime_utc")
    )

    # 4. Rollover detection (before Front filtering)
    rollover = detect_rollover(all_data, front, next_c)
    status = rollover["status"]
    status_color = {
        "IMMINENT": RED, "APPROACHING": YELLOW, "EARLY": CYAN, "STABLE": GREEN
    }.get(status, RESET)

    print(f"\n  🔄 Rollover: {status_color}{BOLD}{status}{RESET}")
    print(f"     {front} vol: {rollover['front_vol']:,}  |  "
          f"{next_c} vol: {rollover['next_vol']:,}  |  "
          f"ratio: {rollover['ratio']:.1%}")

    # 5. Filter Front only for Flux_Data
    front_data = all_data.filter(pl.col("contract") == front)
    other = all_data.shape[0] - front_data.shape[0]
    if other > 0:
        print(f"  Filtered Front ({front}): {other:,} Last/Next ticks removed")

    # 6. Trim 30 days
    if not front_data.is_empty():
        cutoff = front_data["datetime_utc"].max() - timedelta(days=KEEP_DAYS)
        trimmed = front_data.filter(pl.col("datetime_utc") >= cutoff)
        dropped = front_data.shape[0] - trimmed.shape[0]
        if dropped:
            print(f"  Trim {KEEP_DAYS}d: {dropped:,} ticks removed")
    else:
        trimmed = front_data

    # 7. Save
    if not trimmed.is_empty():
        print(f"  Saving...", end=" ", flush=True)
        trimmed.write_parquet(parquet_path(symbol), compression="zstd")
        print("✓")
        print_depth(symbol, trimmed)
    else:
        print(f"  {YELLOW}No Front ticks — Flux_Data empty{RESET}")

    return rollover


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {BOLD}{CYAN}  PULSE — Bridge Scid → Parquet ({KEEP_DAYS}d, Front){RESET}")
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  Source : {SCID_DIR}")
    print(f"  Dest   : {FLUX_DIR}")
    print()

    if not SCID_DIR.exists():
        print(f"  {RED}Scid_Data not found — run pulse_sync_scid.py first{RESET}")
        sys.exit(1)

    # Load contracts.json
    contracts = load_contracts()
    if not contracts:
        print(f"  {RED}contracts.json not found — run pulse_sync_scid.py first{RESET}")
        sys.exit(1)

    # Bridge per symbol
    for symbol in SYMBOLS:
        if symbol not in contracts:
            print(f"  {YELLOW}[{symbol}] Not in contracts.json — skip{RESET}")
            continue
        rollover = bridge_symbol(symbol, contracts[symbol])
        # Add rollover info to contracts.json
        if rollover:
            contracts[symbol]["rollover"] = rollover

            # Auto-flip: if IMMINENT and ratio > 1.0, Next has already overtaken.
            # Flip last=front, front=next, next=following in the calendar.
            # Safety net in case pulse_sync_scid has not yet detected.
            if rollover["status"] == "IMMINENT" and rollover["ratio"] > 1.0:
                old_last  = contracts[symbol]["last"]
                old_front = contracts[symbol]["front"]
                old_next  = contracts[symbol]["next"]
                new_next  = next_contract_after(symbol, old_next)

                print(f"\n  {RED}{BOLD}⚠️  AUTO-FLIP {symbol}{RESET}")
                print(f"     Last  : {old_last} → {old_front}")
                print(f"     Front : {old_front} → {old_next}")
                print(f"     Next  : {old_next} → {new_next}")

                contracts[symbol]["last"]  = old_front
                contracts[symbol]["front"] = old_next
                contracts[symbol]["next"]  = new_next

                # Re-bridge with the new contracts to write the correct Flux_Data
                print(f"  {CYAN}Re-bridging {symbol} with Front={old_next}...{RESET}")
                rollover2 = bridge_symbol(symbol, contracts[symbol])
                if rollover2:
                    contracts[symbol]["rollover"] = rollover2

    # Save updated contracts.json
    save_contracts(contracts)

    # Final summary
    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {BOLD}{GREEN}  Flux_Data — {KEEP_DAYS}d Front contract:{RESET}")
    print(f"  {BOLD}{'═' * 55}{RESET}")
    for symbol in SYMBOLS:
        p = parquet_path(symbol)
        if p.exists():
            df = pl.read_parquet(p)
            print_depth(symbol, df)
        else:
            print(f"  {YELLOW}[{symbol}] Empty{RESET}")
    print()


if __name__ == "__main__":
    main()
