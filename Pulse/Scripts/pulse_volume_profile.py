#!/usr/bin/env python3
"""
pulse_volume_profile.py — Volume Profile per RTH session

Usage:
    python3 Scripts/pulse_volume_profile.py
    python3 Scripts/pulse_volume_profile.py --symbol GC
    python3 Scripts/pulse_volume_profile.py --days 5

Source  : Data/Flux_Data/{symbol}_Tick_Flux.parquet
Produces: Data/Intel_Data/{symbol}_VolumeProfile.parquet

Computes per RTH session:
    - Volume per price level (rounded to the tick)
    - POC (Point of Control) — price with the most volume
    - Value Area (70% of volume) — VAH and VAL
    - Delta per level (bid/ask imbalance)
    - VA migration vs previous session
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("pip3 install polars")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR  = Path(__file__).parent.parent
DATA_DIR  = BASE_DIR / "Data"
FLUX_DIR  = DATA_DIR / "Flux_Data"
INTEL_DIR = DATA_DIR / "Intel_Data"
INTEL_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = ["GC", "NQ"]

TICK_SIZE = {"GC": 0.10, "NQ": 0.25}
NAMES    = {"GC": "Gold COMEX", "NQ": "Nasdaq 100"}

# RTH sessions (UTC)
RTH = {
    "GC": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0},
    "NQ": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0},
}

VALUE_AREA_PCT = 0.70  # 70% of volume

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[90m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def load_ticks(symbol: str, days: int = None) -> pl.DataFrame:
    p = FLUX_DIR / f"{symbol}_Tick_Flux.parquet"
    if not p.exists():
        print(f"  {RED}[{symbol}] Tick_Flux not found{RESET}")
        return pl.DataFrame()
    df = pl.read_parquet(p)
    if days:
        cutoff = df["datetime_utc"].max() - timedelta(days=days)
        df = df.filter(pl.col("datetime_utc") >= cutoff)
    return df


def get_session_dates(df: pl.DataFrame, sym: str) -> list:
    """Return the list of dates with RTH data."""
    rth = RTH[sym]
    return (
        df.filter(
            (pl.col("datetime_utc").dt.hour() >= rth["open_h"]) &
            (pl.col("datetime_utc").dt.hour() < rth["close_h"])
        )
        .with_columns(pl.col("datetime_utc").dt.date().alias("date"))
        ["date"].unique().sort().to_list()
    )


def session_ticks(df: pl.DataFrame, sym: str, date) -> pl.DataFrame:
    """Filter RTH ticks for a given date."""
    rth = RTH[sym]
    start = datetime(date.year, date.month, date.day, rth["open_h"], rth["open_m"])
    end   = datetime(date.year, date.month, date.day, rth["close_h"], rth["close_m"])
    return df.filter(
        (pl.col("datetime_utc") >= start) &
        (pl.col("datetime_utc") <= end)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# VOLUME PROFILE
# ═══════════════════════════════════════════════════════════════════════════════

def compute_profile(ticks: pl.DataFrame, tick_size: float) -> pl.DataFrame:
    """
    Volume profile for a set of ticks.
    Returns a DataFrame with one row per price level:
        price_level | volume | bid_vol | ask_vol | delta | tick_count
    """
    return (
        ticks.with_columns(
            (pl.col("close") / tick_size).round(0).mul(tick_size).alias("price_level")
        )
        .group_by("price_level")
        .agg(
            pl.col("volume").sum().alias("volume"),
            pl.col("bid_vol").sum().alias("bid_vol"),
            pl.col("ask_vol").sum().alias("ask_vol"),
            pl.col("delta").sum().alias("delta"),
            pl.len().alias("tick_count"),
        )
        .sort("price_level")
    )


def find_poc(profile: pl.DataFrame) -> float:
    """Point of Control = price with the most volume."""
    return float(profile.sort("volume", descending=True)["price_level"][0])


def find_value_area(profile: pl.DataFrame, poc: float) -> dict:
    """
    Value Area = zone containing VALUE_AREA_PCT of total volume.
    Expansion from the POC upward and downward, one level at a time,
    always choosing the side with the most volume.
    """
    total_vol = profile["volume"].sum()
    target_vol = total_vol * VALUE_AREA_PCT

    # Index of the POC
    levels = profile.sort("price_level")
    prices = levels["price_level"].to_list()
    volumes = levels["volume"].to_list()

    poc_idx = None
    for i, p in enumerate(prices):
        if abs(p - poc) < 0.001:
            poc_idx = i
            break

    if poc_idx is None:
        return {"vah": poc, "val": poc, "va_vol": 0, "total_vol": total_vol}

    # Expansion
    cum_vol = volumes[poc_idx]
    lo = poc_idx
    hi = poc_idx

    while cum_vol < target_vol and (lo > 0 or hi < len(prices) - 1):
        vol_above = volumes[hi + 1] if hi + 1 < len(prices) else 0
        vol_below = volumes[lo - 1] if lo - 1 >= 0 else 0

        if vol_above >= vol_below and hi + 1 < len(prices):
            hi += 1
            cum_vol += volumes[hi]
        elif lo - 1 >= 0:
            lo -= 1
            cum_vol += volumes[lo]
        else:
            hi += 1
            cum_vol += volumes[hi]

    return {
        "vah": prices[hi],
        "val": prices[lo],
        "va_vol": cum_vol,
        "total_vol": total_vol,
    }


def classify_migration(prev_va: dict, curr_va: dict) -> str:
    """Compare today's VA vs yesterday's."""
    if not prev_va:
        return "FIRST"

    p_vah, p_val = prev_va["vah"], prev_va["val"]
    c_vah, c_val = curr_va["vah"], curr_va["val"]

    if c_val > p_vah:
        return "HIGHER_VALUE"
    elif c_vah < p_val:
        return "LOWER_VALUE"
    elif c_vah > p_vah and c_val >= p_val:
        return "HIGHER_OVERLAP"
    elif c_val < p_val and c_vah <= p_vah:
        return "LOWER_OVERLAP"
    elif c_val >= p_val and c_vah <= p_vah:
        return "INSIDE"
    else:
        return "OUTSIDE"


# ═══════════════════════════════════════════════════════════════════════════════
# PER-SYMBOL ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_symbol(symbol: str, days: int = None):
    print(f"\n  {CYAN}{'─' * 50}{RESET}")
    print(f"  {BOLD}{symbol} — {NAMES[symbol]}{RESET}")
    print(f"  {'─' * 50}")

    df = load_ticks(symbol, days=days)
    if df.is_empty():
        return

    tick = TICK_SIZE[symbol]
    dates = get_session_dates(df, symbol)
    print(f"  {len(dates)} RTH sessions")

    results = []
    prev_va = None

    for date in dates:
        ticks = session_ticks(df, symbol, date)
        if ticks.shape[0] < 100:
            continue

        profile = compute_profile(ticks, tick)
        poc = find_poc(profile)
        va = find_value_area(profile, poc)
        migration = classify_migration(prev_va, va)

        # Session stats
        session_high = float(ticks["close"].max())
        session_low  = float(ticks["close"].min())
        session_open = float(ticks["close"][0])
        session_close = float(ticks["close"][-1])
        net_delta = int(ticks["delta"].sum())
        total_vol = int(ticks["volume"].sum())

        results.append({
            "date": date,
            "contract": ticks["contract"][0] if "contract" in ticks.columns else "",
            "poc": poc,
            "vah": va["vah"],
            "val": va["val"],
            "va_volume": va["va_vol"],
            "total_volume": total_vol,
            "session_high": session_high,
            "session_low": session_low,
            "session_open": session_open,
            "session_close": session_close,
            "net_delta": net_delta,
            "migration": migration,
            "tick_count": ticks.shape[0],
        })

        prev_va = va

    if not results:
        print(f"  {YELLOW}No session with enough data{RESET}")
        return

    # Build the result DataFrame
    out = pl.DataFrame(results)

    # Save
    out_path = INTEL_DIR / f"{symbol}_VolumeProfile.parquet"
    out.write_parquet(out_path, compression="zstd")
    size = out_path.stat().st_size / 1e6
    print(f"  {GREEN}→ {out_path.name} — {out.shape[0]} sessions — {size:.2f} MB{RESET}")

    # Display recent sessions
    print()
    recent = out.tail(7)
    for row in recent.iter_rows(named=True):
        d = row["date"]
        poc = row["poc"]
        vah = row["vah"]
        val = row["val"]
        mig = row["migration"]
        delta = row["net_delta"]
        vol = row["total_volume"]

        mig_color = {
            "HIGHER_VALUE": GREEN, "HIGHER_OVERLAP": GREEN,
            "LOWER_VALUE": RED, "LOWER_OVERLAP": RED,
            "INSIDE": YELLOW, "OUTSIDE": CYAN,
        }.get(mig, DIM)

        d_color = GREEN if delta > 0 else RED if delta < 0 else DIM

        print(f"  {DIM}{d}{RESET}  "
              f"POC {BOLD}{poc:>10,.2f}{RESET}  "
              f"VA [{val:>10,.2f} — {vah:>10,.2f}]  "
              f"{mig_color}{mig:16s}{RESET}  "
              f"Δ{d_color}{delta:>+9,}{RESET}  "
              f"{DIM}vol:{vol:>12,}{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]
    symbol_filter = None
    days = None

    for i, a in enumerate(args):
        if a == "--symbol" and i + 1 < len(args):
            symbol_filter = args[i + 1].upper()
        if a == "--days" and i + 1 < len(args):
            days = int(args[i + 1])

    symbols = [symbol_filter] if symbol_filter else SYMBOLS

    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {BOLD}{CYAN}  PULSE — Volume Profile{RESET}")
    print(f"  {BOLD}{'═' * 55}{RESET}")
    if days:
        print(f"  Window : {days} most recent days")
    print()

    for sym in symbols:
        analyze_symbol(sym, days=days)

    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {GREEN}Done.{RESET}")
    print()


if __name__ == "__main__":
    main()
