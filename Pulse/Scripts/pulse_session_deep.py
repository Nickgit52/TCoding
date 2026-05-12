#!/usr/bin/env python3
"""
pulse_session_deep.py — Phase 6: Multi-session calibration (Asia / Europe / US RTH)

Usage:
    python3 Scripts/pulse_session_deep.py
    python3 Scripts/pulse_session_deep.py --sample 60
    python3 Scripts/pulse_session_deep.py --symbol GC

Do tick signals have an edge outside US RTH?
Calibrates ABSORPTION, DELTA_DIV, BURST, LARGE_PRINT on 3 sessions:
  - ASIA   : 23:00 - 07:00 UTC  (Tokyo open → London open)
  - EUROPE : 07:00 - 13:30 UTC  (London open → US RTH open)
  - US RTH : 13:30 - 20:00 UTC  (already-calibrated baseline)

Also tests:
  - European IB and Opening Drive (8:00-9:00 IB, 30 min OD)
  - Asian IB and Opening Drive (23:00-00:00 IB, 30 min OD)
  - Per-session volume floors (Asia/Europe volume is lower)

Source: Data/Ticks_Parquet_Training/{symbol}_ticks.parquet
"""

import sys
import time
import random
from datetime import datetime, timedelta
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("pip3 install polars")
    sys.exit(1)

# Reuse detections from calibrate
sys.path.insert(0, str(Path(__file__).parent))
from pulse_calibrate import (
    load_training, get_rth_dates,
    detect_large_prints, detect_absorption, detect_iceberg,
    detect_burst, detect_delta_divergence, detect_stacked_imbalance,
    detect_exhaustion,
    measure_forward_returns,
    DEFAULT_THRESHOLDS, TICK_SIZE, MIN_VOLUME,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent.parent
SYMBOLS = ["GC", "NQ"]

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[90m"
RESET  = "\033[0m"

# Measurement horizons (seconds)
HORIZONS = [30, 60, 120]

# Global sessions (UTC)
SESSIONS = {
    "ASIA":   {"start_h": 23, "start_m": 0,  "end_h": 7,  "end_m": 0,  "cross_midnight": True},
    "EUROPE": {"start_h": 7,  "start_m": 0,  "end_h": 13, "end_m": 30, "cross_midnight": False},
    "US_RTH": {"start_h": 13, "start_m": 30, "end_h": 20, "end_m": 0,  "cross_midnight": False},
}

# IB and OD per session
SESSION_IB = {
    "ASIA":   {"ib_start_h": 23, "ib_start_m": 0,  "ib_minutes": 60, "od_minutes": 30},
    "EUROPE": {"ib_start_h": 8,  "ib_start_m": 0,  "ib_minutes": 60, "od_minutes": 30},
    "US_RTH": {"ib_start_h": 13, "ib_start_m": 30, "ib_minutes": 60, "od_minutes": 30},
}

# Volume floors to test per session
VOLUME_FLOORS = {
    "GC": [3, 5, 10],
    "NQ": [5, 10, 20, 30],
}


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def get_all_dates(df: pl.DataFrame) -> list:
    """Return all unique dates in the dataset."""
    return df.with_columns(
        pl.col("datetime_utc").dt.date().alias("date")
    )["date"].unique().sort().to_list()


def session_ticks_global(df: pl.DataFrame, date, session_name: str) -> pl.DataFrame:
    """Filter ticks for a given session (handles Asia cross-midnight)."""
    cfg = SESSIONS[session_name]

    if cfg["cross_midnight"]:
        # Asia: 23:00 date-1 → 07:00 date
        start = datetime(date.year, date.month, date.day, cfg["start_h"], cfg["start_m"]) - timedelta(days=1)
        end = datetime(date.year, date.month, date.day, cfg["end_h"], cfg["end_m"])
    else:
        start = datetime(date.year, date.month, date.day, cfg["start_h"], cfg["start_m"])
        end = datetime(date.year, date.month, date.day, cfg["end_h"], cfg["end_m"])

    return df.filter(
        (pl.col("datetime_utc") >= start) &
        (pl.col("datetime_utc") < end)
    )


def detect_all_signals(ticks_filtered, sym, thresholds):
    """Run all tick-level detections (no session context).
    Detection functions in calibrate already add predicted_dir."""
    tick = TICK_SIZE[sym]
    all_signals = []

    for fn, args in [
        (detect_large_prints, (ticks_filtered, thresholds)),
        (detect_absorption, (ticks_filtered, thresholds)),
        (detect_burst, (ticks_filtered, thresholds)),
        (detect_delta_divergence, (ticks_filtered, sym, thresholds)),
    ]:
        try:
            sigs = fn(*args)
            if not sigs.is_empty():
                all_signals.append(sigs)
        except Exception:
            pass

    if not all_signals:
        return pl.DataFrame()

    combined = pl.concat(all_signals, how="diagonal_relaxed")
    # Ensure predicted_dir exists and filter out neutrals
    if "predicted_dir" in combined.columns:
        combined = combined.filter(pl.col("predicted_dir") != 0)
    return combined


def detect_session_ib_od(ticks: pl.DataFrame, date, session_name: str, symbol: str) -> pl.DataFrame:
    """Detect IB_BREAK and OPENING_DRIVE for a given session."""
    cfg = SESSION_IB[session_name]

    if session_name == "ASIA":
        # Asia IB starts at 23:00 UTC the previous day
        ib_start = datetime(date.year, date.month, date.day, cfg["ib_start_h"], cfg["ib_start_m"]) - timedelta(days=1)
    else:
        ib_start = datetime(date.year, date.month, date.day, cfg["ib_start_h"], cfg["ib_start_m"])

    ib_end = ib_start + timedelta(minutes=cfg["ib_minutes"])
    od_end = ib_start + timedelta(minutes=cfg["od_minutes"])

    # Filter session ticks
    session = ticks
    if session.is_empty() or session.shape[0] < 100:
        return pl.DataFrame()

    all_signals = []

    # Opening Drive
    od = session.filter(
        (pl.col("datetime_utc") >= ib_start) &
        (pl.col("datetime_utc") <= od_end)
    )
    if od.shape[0] >= 10:
        od_open = float(od["close"][0])
        od_close = float(od["close"][-1])
        od_move = od_close - od_open
        od_vol = int(od["volume"].sum())

        min_od = {"GC": 1.0, "NQ": 5.0}.get(symbol, 1.0)
        if abs(od_move) >= min_od:
            direction = "UP" if od_move > 0 else "DOWN"
            all_signals.append({
                "datetime_utc": od["datetime_utc"][-1],
                "close": od_close,
                "volume": od_vol,
                "delta": int(od["delta"].sum()),
                "signal": f"OD_{session_name}_{direction}",
                "score": min(abs(od_move) / (min_od * 3), 1.0),
                "details": f"{session_name} OD {direction} Δpx={od_move:+.2f}",
            })

    # IB
    ib = session.filter(
        (pl.col("datetime_utc") >= ib_start) &
        (pl.col("datetime_utc") <= ib_end)
    )
    if ib.is_empty() or ib.shape[0] < 20:
        if all_signals:
            return pl.DataFrame(all_signals)
        return pl.DataFrame()

    ib_high = float(ib["close"].max())
    ib_low = float(ib["close"].min())

    # Post-IB breaks
    post_ib = session.filter(pl.col("datetime_utc") > ib_end)
    if post_ib.is_empty():
        if all_signals:
            return pl.DataFrame(all_signals)
        return pl.DataFrame()

    ib_broken_up = False
    ib_broken_down = False

    for row in post_ib.iter_rows(named=True):
        if not ib_broken_up and row["close"] > ib_high:
            ib_broken_up = True
            all_signals.append({
                "datetime_utc": row["datetime_utc"],
                "close": row["close"],
                "volume": int(row["volume"]),
                "delta": int(row["delta"]),
                "signal": f"IB_{session_name}_BREAK_UP",
                "score": 0.9,
                "details": f"{session_name} IB=[{ib_low:.2f}-{ib_high:.2f}] break={row['close']:.2f}",
            })
        if not ib_broken_down and row["close"] < ib_low:
            ib_broken_down = True
            all_signals.append({
                "datetime_utc": row["datetime_utc"],
                "close": row["close"],
                "volume": int(row["volume"]),
                "delta": int(row["delta"]),
                "signal": f"IB_{session_name}_BREAK_DOWN",
                "score": 0.7,
                "details": f"{session_name} IB=[{ib_low:.2f}-{ib_high:.2f}] break={row['close']:.2f}",
            })
        if ib_broken_up and ib_broken_down:
            break

    if not all_signals:
        return pl.DataFrame()
    return pl.DataFrame(all_signals)


def print_hit_table_df(title, df, horizons=HORIZONS):
    """Print a hit rate table from a DataFrame with hit_Xs columns."""
    if df.is_empty():
        print(f"  {DIM}(no signal){RESET}")
        return

    h_labels = [f"{h}s" for h in horizons]
    print(f"    {'Signal':25s} {'Count':>7s}  " + "  ".join(f"{l:>7s}" for l in h_labels))
    print(f"    {'─' * 65}")

    # Group by signal
    for sig_name in sorted(df["signal"].unique().to_list()):
        subset = df.filter(pl.col("signal") == sig_name)
        n = subset.shape[0]
        if n < 5:
            continue
        rates = []
        for h in horizons:
            col = f"hit_{h}s"
            if col in subset.columns:
                hits = subset[col].drop_nulls().sum()
                total = subset[col].drop_nulls().len()
                rate = hits / total * 100 if total > 0 else 0
            else:
                rate = 0
            color = GREEN if rate >= 55 else YELLOW if rate >= 52 else RED if rate < 48 else ""
            rst = RESET if color else ""
            rates.append(f"{color}{rate:6.1f}%{rst}")
        print(f"    {sig_name:25s} n={n:>5,}  " + "  ".join(rates))


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 1 — HIT RATE PER SESSION
# ═══════════════════════════════════════════════════════════════════════════════

def test_signals_by_session(all_ticks, sym, dates, sample_dates):
    """Calibrate tick signals on each session."""
    print(f"\n  {'═' * 70}")
    print(f"  {BOLD}T1 — TICK SIGNALS PER SESSION (ASIA / EUROPE / US_RTH){RESET}")
    print(f"  {'═' * 70}")
    print(f"  Same signals, same thresholds — only the time window changes.")

    min_vol = MIN_VOLUME[sym]
    thresholds = DEFAULT_THRESHOLDS.copy()
    thresholds["min_volume"] = min_vol

    for session_name in ["ASIA", "EUROPE", "US_RTH"]:
        print(f"\n  {BOLD}{CYAN}{session_name}{RESET}", end="")
        cfg = SESSIONS[session_name]
        if cfg["cross_midnight"]:
            print(f"  {DIM}({cfg['start_h']:02d}:{cfg['start_m']:02d} → {cfg['end_h']:02d}:{cfg['end_m']:02d} UTC, cross-midnight){RESET}")
        else:
            print(f"  {DIM}({cfg['start_h']:02d}:{cfg['start_m']:02d} → {cfg['end_h']:02d}:{cfg['end_m']:02d} UTC){RESET}")

        all_results = []
        n_sessions = 0

        for date in sample_dates:
            ticks = session_ticks_global(all_ticks, date, session_name)
            if ticks.shape[0] < 200:
                continue

            # Filter by volume
            filtered = ticks.filter(pl.col("volume") >= min_vol)
            if filtered.shape[0] < 50:
                continue

            n_sessions += 1
            signals = detect_all_signals(filtered, sym, thresholds)
            if signals.is_empty():
                continue

            # Measure forward returns on UNFILTERED ticks
            measured = measure_forward_returns(signals, ticks)
            if not measured.is_empty():
                all_results.append(measured)

        if all_results:
            combined = pl.concat(all_results, how="diagonal_relaxed")
            print(f"  {n_sessions} sessions, {combined.shape[0]} signals measured")
            print_hit_table_df(session_name, combined)
        else:
            print(f"  {n_sessions} sessions, 0 signals measured")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 2 — IB AND OPENING DRIVE PER SESSION
# ═══════════════════════════════════════════════════════════════════════════════

def test_ib_od_by_session(all_ticks, sym, dates, sample_dates):
    """Calibrate IB break and Opening Drive on each session."""
    print(f"\n  {'═' * 70}")
    print(f"  {BOLD}T2 — IB BREAK + OPENING DRIVE PER SESSION{RESET}")
    print(f"  {'═' * 70}")
    print(f"  IB = first hour of each session. OD = first 30 minutes.")
    print(f"  Direction = FADE (inverted) as for US RTH.")

    for session_name in ["ASIA", "EUROPE", "US_RTH"]:
        cfg = SESSION_IB[session_name]
        print(f"\n  {BOLD}{CYAN}{session_name}{RESET}  "
              f"{DIM}IB={cfg['ib_start_h']:02d}:{cfg['ib_start_m']:02d}+{cfg['ib_minutes']}min, "
              f"OD={cfg['od_minutes']}min{RESET}")

        all_results = []
        n_sessions = 0

        for date in sample_dates:
            ticks = session_ticks_global(all_ticks, date, session_name)
            if ticks.shape[0] < 200:
                continue

            n_sessions += 1

            # Detect IB/OD
            ib_od = detect_session_ib_od(ticks, date, session_name, sym)
            if ib_od.is_empty():
                continue

            # Add predicted_dir (FADE: UP→-1, DOWN→+1)
            ib_od = ib_od.with_columns(
                pl.when(pl.col("signal").str.contains("UP"))
                .then(pl.lit(-1))
                .otherwise(pl.lit(1))
                .alias("predicted_dir")
            )

            # Measure forward returns
            measured = measure_forward_returns(ib_od, ticks)
            if not measured.is_empty():
                all_results.append(measured)

        if all_results:
            combined = pl.concat(all_results, how="diagonal_relaxed")
            print(f"  {n_sessions} sessions, {combined.shape[0]} signals")
            print_hit_table_df(session_name, combined)
        else:
            print(f"  {n_sessions} sessions, 0 signals")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST 3 — VOLUME FLOOR PER SESSION
# ═══════════════════════════════════════════════════════════════════════════════

def test_volume_floors(all_ticks, sym, dates, sample_dates):
    """Test different MIN_VOLUME per session."""
    print(f"\n  {'═' * 70}")
    print(f"  {BOLD}T3 — VOLUME FLOOR PER SESSION{RESET}")
    print(f"  {'═' * 70}")
    print(f"  Asia/Europe volume is lower. Is the optimal threshold different?")

    floors = VOLUME_FLOORS[sym]
    thresholds = DEFAULT_THRESHOLDS.copy()

    for session_name in ["ASIA", "EUROPE"]:
        print(f"\n  {BOLD}{CYAN}{session_name}{RESET}")

        for floor in floors:
            thresholds["min_volume"] = floor
            all_results = []
            n_signals = 0

            for date in sample_dates:
                ticks = session_ticks_global(all_ticks, date, session_name)
                if ticks.shape[0] < 200:
                    continue

                filtered = ticks.filter(pl.col("volume") >= floor)
                if filtered.shape[0] < 30:
                    continue

                signals = detect_all_signals(filtered, sym, thresholds)
                if signals.is_empty():
                    continue

                n_signals += signals.shape[0]
                measured = measure_forward_returns(signals, ticks)
                if not measured.is_empty():
                    all_results.append(measured)

            if all_results:
                combined = pl.concat(all_results, how="diagonal_relaxed")
                print(f"\n    vol>={floor}  ({combined.shape[0]} signals measured)")
                # Quick summary — only ABSORPTION_BULL and DELTA_DIV_BULL
                for target_sig in ["ABSORPTION_BULL", "DELTA_DIV_BULL"]:
                    subset = combined.filter(pl.col("signal") == target_sig)
                    n = subset.shape[0]
                    if n < 5:
                        continue
                    rates = []
                    for h in HORIZONS:
                        col = f"hit_{h}s"
                        if col in subset.columns:
                            hits = subset[col].drop_nulls().sum()
                            total = subset[col].drop_nulls().len()
                            rate = hits / total * 100 if total > 0 else 0
                        else:
                            rate = 0
                        color = GREEN if rate >= 55 else YELLOW if rate >= 52 else ""
                        rst = RESET if color else ""
                        rates.append(f"{color}{rate:.1f}%{rst}")
                    print(f"      {target_sig:25s} n={n:>5,}  " + "  ".join(rates))
            else:
                print(f"\n    vol>={floor}  (0 signals measured)")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()

    args = sys.argv[1:]
    sample = None
    sym_filter = None

    for i, a in enumerate(args):
        if a == "--sample" and i + 1 < len(args):
            sample = int(args[i + 1])
        if a == "--symbol" and i + 1 < len(args):
            sym_filter = args[i + 1].upper()

    symbols = [sym_filter] if sym_filter else SYMBOLS

    print()
    print(f"  {'═' * 70}")
    print(f"  {BOLD}{CYAN}  PULSE — Phase 6: Multi-Session Deep Exploration{RESET}")
    print(f"  {'═' * 70}")
    print(f"  3 sessions: ASIA (23:00-07:00) / EUROPE (07:00-13:30) / US RTH (13:30-20:00)")
    print(f"  Do tick signals have an edge outside US RTH?")

    for sym in symbols:
        print(f"\n  Loading {sym}_ticks.parquet...", end=" ", flush=True)
        t1 = time.time()
        all_ticks = load_training(sym)
        if all_ticks.is_empty():
            continue
        print(f"{all_ticks.shape[0]:,} ticks in {time.time()-t1:.1f}s")

        dates = get_all_dates(all_ticks)
        print(f"  {len(dates)} dates available")

        if sample:
            sample_dates = sorted(random.sample(dates, min(sample, len(dates))))
            print(f"  Sample: {len(sample_dates)} dates")
        else:
            sample_dates = dates

        # T1 — Tick signals per session
        test_signals_by_session(all_ticks, sym, dates, sample_dates)

        # T2 — IB + OD per session
        test_ib_od_by_session(all_ticks, sym, dates, sample_dates)

        # T3 — Volume floors
        test_volume_floors(all_ticks, sym, dates, sample_dates)

    elapsed = time.time() - t0
    print(f"\n  {'═' * 70}")
    print(f"  Done in {elapsed/60:.0f}m{elapsed%60:.0f}s")
    print()


if __name__ == "__main__":
    main()
