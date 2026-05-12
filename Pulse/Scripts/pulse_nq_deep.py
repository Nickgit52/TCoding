#!/usr/bin/env python3
"""
pulse_nq_deep.py — NQ deep exploration: why do tick signals lack edge?

Usage:
    python3 Scripts/pulse_nq_deep.py
    python3 Scripts/pulse_nq_deep.py --sample 60

5 hypotheses tested:
  H1 — Timing: hit rate per RTH time slice (opening, midday, power hour)
  H2 — Volume floor: MIN_VOLUME at 10, 20, 30, 50 — which threshold isolates institutionals?
  H3 — Windows: NQ trades faster, do windows need to be longer?
  H4 — Confluence: 2+ signals within the same 5-minute window = combined edge?
  H5 — Horizons: forward returns at 5m and 10m in addition to 30/60/120s

Source: Data/Ticks_Parquet_Training/NQ_ticks.parquet
"""

import sys
import time
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
    load_training, get_rth_dates, session_ticks,
    detect_large_prints, detect_absorption, detect_iceberg,
    detect_burst, detect_delta_divergence, detect_stacked_imbalance,
    detect_exhaustion,
    measure_forward_returns,
    DEFAULT_THRESHOLDS, TICK_SIZE, MIN_VOLUME, RTH,
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent.parent
SYM = "NQ"

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[90m"
RESET  = "\033[0m"

# Extended horizons (seconds)
HORIZONS_EXTENDED = [30, 60, 120, 300, 600]  # + 5min, 10min

# RTH time slices (UTC) — NQ RTH = 13:30-20:00
TIME_SLICES = {
    "Opening 13:30-14:30": (13, 30, 14, 30),
    "EU_Close 14:30-16:00": (14, 30, 16, 0),
    "Midday  16:00-18:00": (16, 0, 18, 0),
    "PowerHr 19:00-20:00": (19, 0, 20, 0),
}


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def detect_all_signals(ticks_filtered, ticks_raw, sym, thresholds):
    """Run all detections, return signals + forward returns."""
    tick = TICK_SIZE[sym]
    all_signals = []

    for fn, args in [
        (detect_large_prints, (ticks_filtered, thresholds)),
        (detect_absorption, (ticks_filtered, thresholds)),
        (detect_iceberg, (ticks_filtered, tick, thresholds)),
        (detect_burst, (ticks_filtered, thresholds)),
        (detect_delta_divergence, (ticks_filtered, sym, thresholds)),
        (detect_exhaustion, (ticks_filtered, sym, thresholds)),
    ]:
        try:
            sigs = fn(*args)
            if not sigs.is_empty():
                sigs = sigs.filter(pl.col("predicted_dir") != 0)
                if not sigs.is_empty():
                    all_signals.append(sigs)
        except Exception:
            pass

    if not all_signals:
        return pl.DataFrame()

    combined = pl.concat(all_signals, how="diagonal_relaxed").sort("datetime_utc")
    return combined


def measure_extended_returns(signals, all_ticks, horizons):
    """Measure forward returns with extended horizons."""
    if signals.is_empty():
        return signals

    ticks_ts = all_ticks["datetime_utc"].cast(pl.Int64).to_list()
    ticks_close = all_ticks["close"].to_list()
    n_ticks = len(ticks_ts)

    sig_ts = signals["datetime_utc"].cast(pl.Int64).to_list()
    sig_close = signals["close"].to_list()
    sig_dir = signals["predicted_dir"].to_list()

    results = {h: {"fwd": [], "hit": []} for h in horizons}
    search_idx = 0

    for i in range(len(sig_ts)):
        ts = sig_ts[i]
        price = sig_close[i]
        direction = sig_dir[i]

        while search_idx < n_ticks - 1 and ticks_ts[search_idx] < ts:
            search_idx += 1

        for h in horizons:
            target_ts = ts + h * 1_000_000
            j = search_idx
            while j < n_ticks - 1 and ticks_ts[j] < target_ts:
                j += 1

            if j < n_ticks and abs(ticks_ts[j] - target_ts) < 60_000_000:
                fwd_price = ticks_close[j]
                move = fwd_price - price
                hit = 1 if (direction > 0 and move > 0) or (direction < 0 and move < 0) else 0
            else:
                fwd_price = None
                hit = None

            results[h]["fwd"].append(fwd_price)
            results[h]["hit"].append(hit)

    for h in horizons:
        signals = signals.with_columns([
            pl.Series(f"fwd_{h}s", results[h]["fwd"]),
            pl.Series(f"hit_{h}s", results[h]["hit"]),
        ])

    return signals


def hit_rate(df, horizon):
    """Compute hit rate for a given horizon."""
    col = f"hit_{horizon}s"
    if col not in df.columns:
        return None, 0
    valid = df.filter(pl.col(col).is_not_null())
    if valid.shape[0] == 0:
        return None, 0
    return valid[col].mean() * 100, valid.shape[0]


def print_hit_table(label, df, horizons, signals_filter=None):
    """Print a hit rate table per signal."""
    if df.is_empty():
        print(f"    {DIM}(empty){RESET}")
        return

    if signals_filter:
        sigs = signals_filter
    else:
        sigs = df["signal"].unique().sort().to_list()

    # BULL signals only (those with potential edge)
    bull_sigs = [s for s in sigs if "BULL" in s or "LARGE" in s or "BURST" in s or "EXHAUSTION" in s]
    bear_sigs = [s for s in sigs if "BEAR" in s]

    for sig_list, tag in [(bull_sigs, "BULL/REVERSAL↓"), (bear_sigs, "BEAR/REVERSAL↑")]:
        if not sig_list:
            continue
        for sig in sig_list:
            sub = df.filter(pl.col("signal") == sig)
            if sub.shape[0] == 0:
                continue
            rates = []
            for h in horizons:
                r, n = hit_rate(sub, h)
                if r is not None:
                    color = GREEN if r >= 55 else YELLOW if r >= 50 else RED
                    rates.append(f"{color}{r:5.1f}%{RESET}")
                else:
                    rates.append(f"  n/a ")
            n = sub.shape[0]
            h_str = "  ".join(rates)
            print(f"    {sig:20s}  n={n:>6,}  {h_str}")


# ═══════════════════════════════════════════════════════════════════════════════
# HYPOTHESIS 1 — TIMING (hit rate per time slice)
# ═══════════════════════════════════════════════════════════════════════════════

def test_h1_timing(sessions_data, raw_sessions):
    print(f"\n  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{CYAN}H1 — TIMING: hit rate per RTH time slice{RESET}")
    print(f"  {'═' * 70}")
    print(f"  {DIM}NQ RTH = 13:30-20:00 UTC. Do signals have more edge at certain hours?{RESET}")

    thresholds = DEFAULT_THRESHOLDS.copy()

    for slice_name, (h1, m1, h2, m2) in TIME_SLICES.items():
        all_sigs = []

        for i, (sess_f, sess_r) in enumerate(zip(sessions_data, raw_sessions)):
            # Filter the time slice
            slice_f = sess_f.filter(
                (pl.col("datetime_utc").dt.hour() > h1) |
                ((pl.col("datetime_utc").dt.hour() == h1) & (pl.col("datetime_utc").dt.minute() >= m1))
            ).filter(
                (pl.col("datetime_utc").dt.hour() < h2) |
                ((pl.col("datetime_utc").dt.hour() == h2) & (pl.col("datetime_utc").dt.minute() < m2))
            )

            if slice_f.shape[0] < 200:
                continue

            sigs = detect_all_signals(slice_f, sess_r, SYM, thresholds)
            if not sigs.is_empty():
                sigs = measure_extended_returns(sigs, sess_r, [30, 60, 120])
                all_sigs.append(sigs)

        if all_sigs:
            combined = pl.concat(all_sigs, how="diagonal_relaxed")
            n = combined.shape[0]
            print(f"\n  {BOLD}{slice_name}{RESET}  ({n:,} signals)")
            h_labels = "  ".join([f"{'30s':>6s}", f"{'60s':>6s}", f"{'120s':>6s}"])
            print(f"    {'Signal':20s}  {'Count':>8s}  {h_labels}")
            print(f"    {'─' * 60}")
            print_hit_table(slice_name, combined, [30, 60, 120])
        else:
            print(f"\n  {BOLD}{slice_name}{RESET}  {DIM}(no signal){RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# HYPOTHESIS 2 — VOLUME FLOOR (MIN_VOLUME at 10, 20, 30, 50)
# ═══════════════════════════════════════════════════════════════════════════════

def test_h2_volume(raw_sessions):
    print(f"\n  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{CYAN}H2 — VOLUME FLOOR: which threshold isolates NQ institutionals?{RESET}")
    print(f"  {'═' * 70}")
    print(f"  {DIM}Current: vol≥10 (keeps 13.9%). Let's test vol≥20, 30, 50.{RESET}")

    thresholds = DEFAULT_THRESHOLDS.copy()
    vol_levels = [10, 20, 30, 50]

    for min_vol in vol_levels:
        all_sigs = []
        total_ticks = 0
        kept_ticks = 0

        for sess_r in raw_sessions:
            total_ticks += sess_r.shape[0]
            sess_f = sess_r.filter(pl.col("volume") >= min_vol)
            kept_ticks += sess_f.shape[0]

            if sess_f.shape[0] < 200:
                continue

            sigs = detect_all_signals(sess_f, sess_r, SYM, thresholds)
            if not sigs.is_empty():
                sigs = measure_extended_returns(sigs, sess_r, [30, 60, 120])
                all_sigs.append(sigs)

        pct = kept_ticks / total_ticks * 100 if total_ticks > 0 else 0

        if all_sigs:
            combined = pl.concat(all_sigs, how="diagonal_relaxed")
            n = combined.shape[0]
            # Average volume of signals
            avg_vol = combined["volume"].mean()
            print(f"\n  {BOLD}vol≥{min_vol}{RESET}  ({pct:.1f}% ticks kept, {n:,} signals, vol_avg={avg_vol:.0f})")
            h_labels = "  ".join([f"{'30s':>6s}", f"{'60s':>6s}", f"{'120s':>6s}"])
            print(f"    {'Signal':20s}  {'Count':>8s}  {h_labels}")
            print(f"    {'─' * 60}")
            print_hit_table(f"vol≥{min_vol}", combined, [30, 60, 120])
        else:
            print(f"\n  {BOLD}vol≥{min_vol}{RESET}  {DIM}(no signal){RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# HYPOTHESIS 3 — WINDOWS (larger absorption_window and delta_div_window)
# ═══════════════════════════════════════════════════════════════════════════════

def test_h3_windows(sessions_data, raw_sessions):
    print(f"\n  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{CYAN}H3 — WINDOWS: does NQ need longer windows?{RESET}")
    print(f"  {'═' * 70}")
    print(f"  {DIM}Current: absorption=30, delta_div=200. Let's test 60/80 and 400/600.{RESET}")

    window_tests = [
        ("ABS_w=30 (current)",  {"absorption_window": 30}),
        ("ABS_w=60",            {"absorption_window": 60}),
        ("ABS_w=80",            {"absorption_window": 80}),
        ("ABS_w=120",           {"absorption_window": 120}),
    ]

    thresholds_base = DEFAULT_THRESHOLDS.copy()

    for label, overrides in window_tests:
        thresholds = thresholds_base.copy()
        thresholds.update(overrides)

        all_sigs = []
        for sess_f, sess_r in zip(sessions_data, raw_sessions):
            if sess_f.shape[0] < 200:
                continue
            # Absorption only for this test
            ab = detect_absorption(sess_f, thresholds)
            if not ab.is_empty():
                ab = ab.filter(pl.col("predicted_dir") != 0)
                if not ab.is_empty():
                    ab = measure_extended_returns(ab, sess_r, [30, 60, 120])
                    all_sigs.append(ab)

        if all_sigs:
            combined = pl.concat(all_sigs, how="diagonal_relaxed")
            n = combined.shape[0]
            print(f"\n  {BOLD}{label}{RESET}  ({n:,} signals)")
            h_labels = "  ".join([f"{'30s':>6s}", f"{'60s':>6s}", f"{'120s':>6s}"])
            print(f"    {'Signal':20s}  {'Count':>8s}  {h_labels}")
            print(f"    {'─' * 60}")
            print_hit_table(label, combined, [30, 60, 120])


# ═══════════════════════════════════════════════════════════════════════════════
# HYPOTHESIS 4 — CONFLUENCE (2+ signals within 5 min = combined edge?)
# ═══════════════════════════════════════════════════════════════════════════════

def test_h4_confluence(sessions_data, raw_sessions):
    print(f"\n  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{CYAN}H4 — CONFLUENCE: 2+ signals within 5 min = more edge?{RESET}")
    print(f"  {'═' * 70}")
    print(f"  {DIM}Compare isolated signals vs signals with confirmation within ±5 min.{RESET}")

    thresholds = DEFAULT_THRESHOLDS.copy()
    confluence_window_us = 5 * 60 * 1_000_000  # 5 min in microseconds

    all_isolated = []
    all_confluent = []

    for sess_f, sess_r in zip(sessions_data, raw_sessions):
        if sess_f.shape[0] < 200:
            continue

        sigs = detect_all_signals(sess_f, sess_r, SYM, thresholds)
        if sigs.is_empty() or sigs.shape[0] < 2:
            continue

        sigs = measure_extended_returns(sigs, sess_r, [30, 60, 120])

        # Mark confluence: for each signal, count neighbors within ±5min
        ts_list = sigs["datetime_utc"].cast(pl.Int64).to_list()
        sig_names = sigs["signal"].to_list()
        dirs = sigs["predicted_dir"].to_list()

        confluent_mask = []
        for i in range(len(ts_list)):
            neighbors = 0
            same_dir = 0
            for j in range(len(ts_list)):
                if i == j:
                    continue
                if abs(ts_list[j] - ts_list[i]) <= confluence_window_us:
                    # Different signal (not the same type)
                    if sig_names[j] != sig_names[i]:
                        neighbors += 1
                        if dirs[j] == dirs[i]:
                            same_dir += 1
            confluent_mask.append(neighbors >= 1 and same_dir >= 1)

        sigs = sigs.with_columns(
            pl.Series("is_confluent", confluent_mask)
        )

        isolated = sigs.filter(~pl.col("is_confluent"))
        confluent = sigs.filter(pl.col("is_confluent"))

        if not isolated.is_empty():
            all_isolated.append(isolated)
        if not confluent.is_empty():
            all_confluent.append(confluent)

    # Results
    for label, parts in [("ISOLATED (single signal)", all_isolated),
                         ("CONFLUENT (2+ signals ±5min, same dir)", all_confluent)]:
        if parts:
            combined = pl.concat(parts, how="diagonal_relaxed")
            n = combined.shape[0]
            print(f"\n  {BOLD}{label}{RESET}  ({n:,} signals)")
            h_labels = "  ".join([f"{'30s':>6s}", f"{'60s':>6s}", f"{'120s':>6s}"])
            print(f"    {'Signal':20s}  {'Count':>8s}  {h_labels}")
            print(f"    {'─' * 60}")
            print_hit_table(label, combined, [30, 60, 120])
        else:
            print(f"\n  {BOLD}{label}{RESET}  {DIM}(none){RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# HYPOTHESIS 5 — EXTENDED HORIZONS (5min, 10min)
# ═══════════════════════════════════════════════════════════════════════════════

def test_h5_horizons(sessions_data, raw_sessions):
    print(f"\n  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{CYAN}H5 — HORIZONS: does NQ need more time to react?{RESET}")
    print(f"  {'═' * 70}")
    print(f"  {DIM}Current: 30s, 60s, 120s. Let's add 300s (5min) and 600s (10min).{RESET}")

    thresholds = DEFAULT_THRESHOLDS.copy()
    all_sigs = []

    for sess_f, sess_r in zip(sessions_data, raw_sessions):
        if sess_f.shape[0] < 200:
            continue

        sigs = detect_all_signals(sess_f, sess_r, SYM, thresholds)
        if not sigs.is_empty():
            sigs = measure_extended_returns(sigs, sess_r, HORIZONS_EXTENDED)
            all_sigs.append(sigs)

    if all_sigs:
        combined = pl.concat(all_sigs, how="diagonal_relaxed")
        n = combined.shape[0]
        print(f"\n  {BOLD}All NQ signals{RESET}  ({n:,} signals)")
        h_labels = "  ".join([f"{h}s" if h < 300 else f"{h//60}m" for h in HORIZONS_EXTENDED])
        print(f"    {'Signal':20s}  {'Count':>8s}  " + h_labels)
        print(f"    {'─' * 75}")
        print_hit_table("H5", combined, HORIZONS_EXTENDED)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]
    sample_days = None

    for i, a in enumerate(args):
        if a == "--sample" and i + 1 < len(args):
            sample_days = int(args[i + 1])

    print()
    print(f"  {BOLD}{'═' * 70}{RESET}")
    print(f"  {BOLD}{CYAN}  PULSE — NQ Deep Exploration{RESET}")
    print(f"  {BOLD}{'═' * 70}{RESET}")
    print(f"  5 hypotheses to find NQ's hidden edge")
    if sample_days:
        print(f"  Sample: {sample_days} sessions")
    print()

    t0 = time.time()

    # Load data
    df = load_training(SYM)
    if df.is_empty():
        return

    dates = get_rth_dates(df, SYM)
    print(f"  {len(dates)} RTH sessions available")

    if sample_days and sample_days < len(dates):
        import random
        random.seed(42)
        dates = sorted(random.sample(dates, sample_days))
        print(f"  {YELLOW}Sample: {sample_days} sessions{RESET}")

    # Prepare sessions
    print(f"  Preparing sessions...", end=" ", flush=True)
    sessions_raw = []
    sessions_filtered = []
    min_vol = MIN_VOLUME[SYM]

    for date in dates:
        ticks = session_ticks(df, SYM, date)
        if ticks.shape[0] < 500:
            continue
        sessions_raw.append(ticks)
        sessions_filtered.append(ticks.filter(pl.col("volume") >= min_vol))

    print(f"{len(sessions_raw)} sessions ready")

    # Run the 5 hypotheses
    test_h1_timing(sessions_filtered, sessions_raw)
    test_h2_volume(sessions_raw)
    test_h3_windows(sessions_filtered, sessions_raw)
    test_h4_confluence(sessions_filtered, sessions_raw)
    test_h5_horizons(sessions_filtered, sessions_raw)

    elapsed = time.time() - t0
    m, s = divmod(int(elapsed), 60)

    print()
    print(f"  {BOLD}{'═' * 70}{RESET}")
    print(f"  {GREEN}Done in {m}m{s:02d}s{RESET}")
    print()


if __name__ == "__main__":
    main()
