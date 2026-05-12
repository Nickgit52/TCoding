#!/usr/bin/env python3
"""
pulse_institutional.py — Detect the institutional hand on raw ticks

Usage:
    python3 Scripts/pulse_institutional.py
    python3 Scripts/pulse_institutional.py --symbol GC
    python3 Scripts/pulse_institutional.py --days 5

Source  : /Users/m8raven/Documents/Projets/TCoding/Pulse/Data/Flux_Data/{symbol}_Tick_Flux.parquet
Produces: /Users/m8raven/Documents/Projets/TCoding/Pulse/Data/Intel_Data/{symbol}_Institutional.parquet

8 signal families:
  1. LARGE_PRINT      — tick with abnormally high volume
  2. ABSORPTION       — price↑/↓ but delta diverges → REVERSAL
  3. ICEBERG          — volume concentrated at the same level tick after tick
  4. BURST            — sudden burst of volume + speed
  5. DELTA_DIVERGENCE — macro price/cumulative-delta divergence (Phase 3)
  6. STACKED_IMBALANCE — consecutive levels with bid/ask imbalance (Phase 3)
  7. EXHAUSTION       — large volume without price movement = wall (Phase 3b)
  8. SESSION_CONTEXT  — IB break, opening drive (Phase 3b)

Output per signaled tick:
    datetime_utc | contract | price | volume | delta | signal | score | details
"""

import sys
from datetime import datetime
from typing import Optional
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
FLUX_DIR  = DATA_DIR / "Flux_Data"
INTEL_DIR = DATA_DIR / "Intel_Data"
INTEL_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = ["GC", "NQ"]

TICK_SIZE = {"GC": 0.10, "NQ": 0.25}

# RTH sessions (UTC)
RTH = {
    "GC": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0},
    "NQ": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0},
}

# Phase 6 — Multi-session (Asia / Europe / US RTH)
GLOBAL_SESSIONS = {
    "ASIA":   {"open_h": 23, "open_m": 0,  "close_h": 7,  "close_m": 0,  "cross_midnight": True},
    "EUROPE": {"open_h": 7,  "open_m": 0,  "close_h": 13, "close_m": 30, "cross_midnight": False},
    "US_RTH": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0,  "cross_midnight": False},
}
# IB start per session (first hour)
SESSION_IB_CONFIG = {
    "ASIA":   {"ib_start_h": 23, "ib_start_m": 0,  "ib_minutes": 60, "od_minutes": 30},
    "EUROPE": {"ib_start_h": 8,  "ib_start_m": 0,  "ib_minutes": 60, "od_minutes": 30},
    "US_RTH": {"ib_start_h": 13, "ib_start_m": 30, "ib_minutes": 60, "od_minutes": 30},
}

# Minimum volume per tick to consider an institutional signal
# An institutional doesn't leave a footprint on 1 lot
MIN_VOLUME = {"GC": 5, "NQ": 20}   # NQ raised from 10→20 (Phase 5: DDV_BULL +3pts at vol≥20)

# Analysis windows (in number of ticks)
WINDOW_FAST  = 50    # Fast context — ~30 seconds in active market
WINDOW_SLOW  = 200   # Slow context — ~2 minutes

# Detection thresholds
LARGE_PRINT_Z    = 3.0   # Volume z-score for Large Print
BURST_VOL_Z      = 2.0   # Volume z-score for Burst (calibrated: 66.7% GC at 120s)
BURST_SPEED_Z    = 2.0   # Speed z-score for Burst (ticks/sec)
ICEBERG_WINDOW   = 20    # Tick window to detect level repetition
ICEBERG_MIN_HITS = 6     # Minimum hits at the same level (calibrated: 55.3% GC at 120s)
ABSORPTION_WINDOW = 40   # Tick window for delta/price divergence (calibrated: 62.1% GC, 60.8% NQ)

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def parquet_path(symbol: str) -> Path:
    return FLUX_DIR / f"{symbol}_Tick_Flux.parquet"

def output_path(symbol: str) -> Path:
    return INTEL_DIR / f"{symbol}_Institutional.parquet"

def load_ticks(symbol: str, days: int = None) -> pl.DataFrame:
    p = parquet_path(symbol)
    if not p.exists():
        print(f"  {RED}[{symbol}] Parquet not found — run pulse_listen.py first{RESET}")
        return pl.DataFrame()
    df = pl.read_parquet(p)
    if days:
        cutoff = df["datetime_utc"].max() - pl.duration(days=days)
        df = df.filter(pl.col("datetime_utc") >= cutoff)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 1 — LARGE PRINT
# ═══════════════════════════════════════════════════════════════════════════════

def detect_large_prints(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ticks with abnormally high volume vs recent context.
    Z-score > LARGE_PRINT_Z on a rolling WINDOW_SLOW window.
    An institutional must execute a large size — it leaves
    volume footprints that are impossible to hide.
    """
    df = df.with_columns([
        pl.col("volume").cast(pl.Float64).rolling_mean(WINDOW_SLOW).alias("vol_ma"),
        pl.col("volume").cast(pl.Float64).rolling_std(WINDOW_SLOW).alias("vol_std"),
    ])
    df = df.with_columns([
        ((pl.col("volume").cast(pl.Float64) - pl.col("vol_ma")) /
         (pl.col("vol_std") + 1.0)).alias("vol_z"),
    ])

    signals = df.filter(pl.col("vol_z") >= LARGE_PRINT_Z).with_columns([
        pl.lit("LARGE_PRINT").alias("signal"),
        (pl.col("vol_z") / 6.0).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("vol="), pl.col("volume").cast(pl.Utf8),
            pl.lit(" z="), pl.col("vol_z").round(2).cast(pl.Utf8),
        ])).alias("details"),
    ])

    return signals


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 2 — ABSORPTION
# ═══════════════════════════════════════════════════════════════════════════════

def detect_absorption(df: pl.DataFrame) -> pl.DataFrame:
    """
    Price rises but delta diverges → REVERSAL signal.
    Phase 2 calibration confirms: absorption predicts reversal, not continuation.

    ABSORPTION_BULL: price↑ delta↓ → passive seller absorbs → REVERSAL DOWN (55-61% hit rate at 120s)
    ABSORPTION_BEAR: price↓ delta↑ → passive buyer absorbs  → REVERSAL UP   (50-51% hit rate at 120s)

    Logic: on an N-tick window,
    - price_change = close[-1] - close[-N]
    - delta_cum = sum(delta over N ticks)
    - Divergence: price_change > 0 but delta_cum < 0 → bearish reversal
                  price_change < 0 but delta_cum > 0 → bullish reversal
    """
    w = ABSORPTION_WINDOW

    df = df.with_columns([
        pl.col("close").cast(pl.Float64).alias("close_f"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ])

    df = df.with_columns([
        (pl.col("close_f") - pl.col("close_f").shift(w)).alias("price_change"),
        pl.col("delta_f").rolling_sum(w).alias("delta_cum"),
        pl.col("volume").cast(pl.Float64).rolling_mean(w).alias("vol_ma_abs"),
    ])

    # Bull absorption: price rises, negative delta → REVERSAL DOWN
    # Passive seller absorbs aggressive buyers, the move exhausts
    abs_bull = df.filter(
        (pl.col("price_change") > 0) &
        (pl.col("delta_cum") < 0) &
        (pl.col("vol_ma_abs") > df["volume"].cast(pl.Float64).mean() * 0.5)
    ).with_columns([
        pl.lit("ABSORPTION_BULL").alias("signal"),
        (pl.col("price_change").abs() * pl.col("delta_cum").abs() /
         (pl.col("vol_ma_abs") * 100 + 1)).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("REVERSAL↓ price_chg="), pl.col("price_change").round(2).cast(pl.Utf8),
            pl.lit(" delta_cum="), pl.col("delta_cum").round(0).cast(pl.Utf8),
        ])).alias("details"),
    ])

    # Bear absorption: price falls, positive delta → REVERSAL UP
    # Passive buyer absorbs aggressive sellers, the move exhausts
    abs_bear = df.filter(
        (pl.col("price_change") < 0) &
        (pl.col("delta_cum") > 0) &
        (pl.col("vol_ma_abs") > df["volume"].cast(pl.Float64).mean() * 0.5)
    ).with_columns([
        pl.lit("ABSORPTION_BEAR").alias("signal"),
        (pl.col("price_change").abs() * pl.col("delta_cum").abs() /
         (pl.col("vol_ma_abs") * 100 + 1)).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("REVERSAL↑ price_chg="), pl.col("price_change").round(2).cast(pl.Utf8),
            pl.lit(" delta_cum="), pl.col("delta_cum").round(0).cast(pl.Utf8),
        ])).alias("details"),
    ])

    return pl.concat([abs_bull, abs_bear])


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 3 — ICEBERG
# ═══════════════════════════════════════════════════════════════════════════════

def detect_iceberg(df: pl.DataFrame, tick_size: float) -> pl.DataFrame:
    """
    Massive volume repeated at the same price level.
    An iceberg order regenerates at the same price — the market
    keeps coming back to it with volume.

    Logic: round close to tick, count hits on a rolling window,
    filter if volume is high.
    """
    df = df.with_columns([
        (pl.col("close") / tick_size).round(0).cast(pl.Int64).alias("price_level"),
        pl.col("volume").cast(pl.Float64).rolling_mean(WINDOW_SLOW).alias("vol_ma_ice"),
    ])

    # Count how many times this price level has been hit within the window
    df = df.with_columns([
        pl.col("price_level").rolling_sum(ICEBERG_WINDOW).alias("_dummy"),  # force materialization
    ])

    # For each tick, count hits at the same level in the previous window
    # Approach: shift + compare
    hits_cols = []
    for i in range(1, ICEBERG_WINDOW + 1):
        hits_cols.append(
            (pl.col("price_level") == pl.col("price_level").shift(i)).cast(pl.Int32).alias(f"_hit_{i}")
        )

    df = df.with_columns(hits_cols)
    hit_sum_expr = sum(pl.col(f"_hit_{i}") for i in range(1, ICEBERG_WINDOW + 1))
    df = df.with_columns(hit_sum_expr.alias("level_hits"))

    # Clean up temporary columns
    drop_cols = [f"_hit_{i}" for i in range(1, ICEBERG_WINDOW + 1)] + ["_dummy"]
    df = df.drop([c for c in drop_cols if c in df.columns])

    signals = df.filter(
        (pl.col("level_hits") >= ICEBERG_MIN_HITS) &
        (pl.col("volume").cast(pl.Float64) >= pl.col("vol_ma_ice") * 0.8)
    ).with_columns([
        pl.lit("ICEBERG").alias("signal"),
        (pl.col("level_hits").cast(pl.Float64) / ICEBERG_WINDOW).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("level="), (pl.col("price_level").cast(pl.Float64) * tick_size).round(2).cast(pl.Utf8),
            pl.lit(" hits="), pl.col("level_hits").cast(pl.Utf8),
            pl.lit("/"), pl.lit(str(ICEBERG_WINDOW)),
        ])).alias("details"),
    ])

    return signals


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 4 — BURST
# ═══════════════════════════════════════════════════════════════════════════════

def detect_burst(df: pl.DataFrame) -> pl.DataFrame:
    """
    Sudden burst of volume AND speed (ticks/sec).
    Retail is slow and fragmented. An institutional that
    decides to enter creates a burst impossible to mistake.

    Speed = 1 / (delta_time in seconds between consecutive ticks).
    """
    df = df.with_columns([
        pl.col("datetime_utc").cast(pl.Int64).alias("ts_us"),
        pl.col("volume").cast(pl.Float64).alias("vol_f"),
    ])

    # Time delta between ticks (microseconds → seconds)
    df = df.with_columns([
        ((pl.col("ts_us") - pl.col("ts_us").shift(1)).cast(pl.Float64) / 1_000_000.0
         ).alias("dt_sec"),
    ])

    # Speed: 1/dt (ticks/sec) — clip to avoid division by zero
    df = df.with_columns([
        (1.0 / (pl.col("dt_sec").clip(0.001, None))).alias("tick_speed"),
    ])

    # Z-scores
    df = df.with_columns([
        pl.col("vol_f").rolling_mean(WINDOW_SLOW).alias("vol_ma_b"),
        pl.col("vol_f").rolling_std(WINDOW_SLOW).alias("vol_std_b"),
        pl.col("tick_speed").rolling_mean(WINDOW_SLOW).alias("speed_ma"),
        pl.col("tick_speed").rolling_std(WINDOW_SLOW).alias("speed_std"),
    ])
    df = df.with_columns([
        ((pl.col("vol_f") - pl.col("vol_ma_b")) / (pl.col("vol_std_b") + 1.0)).alias("vol_z_b"),
        ((pl.col("tick_speed") - pl.col("speed_ma")) / (pl.col("speed_std") + 0.01)).alias("speed_z"),
    ])

    signals = df.filter(
        (pl.col("vol_z_b") >= BURST_VOL_Z) &
        (pl.col("speed_z") >= BURST_SPEED_Z)
    ).with_columns([
        pl.lit("BURST").alias("signal"),
        ((pl.col("vol_z_b") + pl.col("speed_z")) / 10.0).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("vol_z="), pl.col("vol_z_b").round(2).cast(pl.Utf8),
            pl.lit(" speed_z="), pl.col("speed_z").round(2).cast(pl.Utf8),
            pl.lit(" dt="), pl.col("dt_sec").round(3).cast(pl.Utf8), pl.lit("s"),
        ])).alias("details"),
    ])

    return signals


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 5 — DELTA DIVERGENCE (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

# Wider window than Absorption — macro view of the session
DELTA_DIV_WINDOW   = 200   # ~2 minutes of ticks
DELTA_DIV_MIN_MOVE = {"GC": 1.0, "NQ": 5.0}  # Minimum price move to consider

def detect_delta_divergence(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    """
    Divergence between price and cumulative delta over a wide window.

    When price makes a new local high but the cumulative delta
    does not follow (or falls), aggressive buyers are exhausting.
    This is the macro view of Absorption — same signal family.

    DELTA_DIV_BULL: price↑ significant but delta_cum↓ → buyer exhaustion → REVERSAL↓
    DELTA_DIV_BEAR: price↓ significant but delta_cum↑ → seller exhaustion → REVERSAL↑
    """
    w = DELTA_DIV_WINDOW
    min_move = DELTA_DIV_MIN_MOVE.get(symbol, 1.0)

    df = df.with_columns([
        pl.col("close").cast(pl.Float64).alias("close_f"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ])

    df = df.with_columns([
        (pl.col("close_f") - pl.col("close_f").shift(w)).alias("price_move"),
        pl.col("delta_f").rolling_sum(w).alias("delta_cum"),
        pl.col("volume").cast(pl.Float64).rolling_sum(w).alias("vol_cum"),
    ])

    # Bull divergence: price rises significantly but cumulative delta negative
    div_bull = df.filter(
        (pl.col("price_move") >= min_move) &
        (pl.col("delta_cum") < 0) &
        (pl.col("vol_cum") > 0)
    ).with_columns([
        pl.lit("DELTA_DIV_BULL").alias("signal"),
        # Score: divergence strength = |price_move| * |delta_cum| / vol_cum
        (pl.col("price_move").abs() * pl.col("delta_cum").abs() /
         (pl.col("vol_cum") + 1)).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("REVERSAL↓ Δpx=+"), pl.col("price_move").round(2).cast(pl.Utf8),
            pl.lit(" Δcum="), pl.col("delta_cum").round(0).cast(pl.Utf8),
            pl.lit(" vol="), pl.col("vol_cum").round(0).cast(pl.Utf8),
        ])).alias("details"),
    ])

    # Bear divergence: price falls significantly but cumulative delta positive
    div_bear = df.filter(
        (pl.col("price_move") <= -min_move) &
        (pl.col("delta_cum") > 0) &
        (pl.col("vol_cum") > 0)
    ).with_columns([
        pl.lit("DELTA_DIV_BEAR").alias("signal"),
        (pl.col("price_move").abs() * pl.col("delta_cum").abs() /
         (pl.col("vol_cum") + 1)).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("REVERSAL↑ Δpx="), pl.col("price_move").round(2).cast(pl.Utf8),
            pl.lit(" Δcum=+"), pl.col("delta_cum").round(0).cast(pl.Utf8),
            pl.lit(" vol="), pl.col("vol_cum").round(0).cast(pl.Utf8),
        ])).alias("details"),
    ])

    parts = [p for p in [div_bull, div_bear] if not p.is_empty()]
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 6 — STACKED IMBALANCE (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

IMBALANCE_RATIO     = 3.0   # Minimum bid/ask or ask/bid ratio for an imbalanced level
IMBALANCE_STACK_MIN = 3     # Minimum consecutive imbalanced levels
IMBALANCE_WINDOW    = 50    # Ticks to look back to profile levels

def detect_stacked_imbalance(df: pl.DataFrame, tick_size: float) -> pl.DataFrame:
    """
    Detect stacked imbalances — 3+ consecutive price levels
    where the bid/ask (or ask/bid) ratio exceeds a threshold.

    A stacked imbalance = strong directional pressure.
    Buyers (or sellers) dominate across several consecutive levels.

    Approach: for each tick, look at the N preceding ticks,
    profile bid_vol and ask_vol per price level, then search for
    sequences of levels imbalanced in the same direction.
    """
    # Round to the tick
    df = df.with_columns([
        (pl.col("close") / tick_size).round(0).cast(pl.Int64).alias("price_level"),
    ])

    # Profile by price level over the entire window.
    # We work tick by tick — for each tick we look at
    # the IMBALANCE_WINDOW preceding ticks
    prices = df["price_level"].to_list()
    bid_vols = df["bid_vol"].to_list()
    ask_vols = df["ask_vol"].to_list()
    times = df["datetime_utc"].to_list()
    closes = df["close"].to_list()
    volumes = df["volume"].to_list()
    deltas = df["delta"].to_list()

    n = len(prices)
    w = IMBALANCE_WINDOW
    min_stack = IMBALANCE_STACK_MIN
    ratio_thresh = IMBALANCE_RATIO

    sig_indices = []
    sig_signals = []
    sig_scores = []
    sig_details = []

    # Sample — check every W-th tick for speed
    for i in range(w, n, w // 2):
        # Profile levels within the window [i-w, i]
        level_bid = {}
        level_ask = {}
        for j in range(max(0, i - w), i + 1):
            lvl = prices[j]
            level_bid[lvl] = level_bid.get(lvl, 0) + bid_vols[j]
            level_ask[lvl] = level_ask.get(lvl, 0) + ask_vols[j]

        # Sort levels by price
        sorted_levels = sorted(level_bid.keys())
        if len(sorted_levels) < min_stack:
            continue

        # Search for imbalanced sequences
        # +1 = ask dominant (aggressive buyers), -1 = bid dominant (aggressive sellers)
        imb_seq = []
        for lvl in sorted_levels:
            b = level_bid.get(lvl, 0)
            a = level_ask.get(lvl, 0)
            if a > 0 and b > 0:
                if a / b >= ratio_thresh:
                    imb_seq.append((lvl, +1))
                elif b / a >= ratio_thresh:
                    imb_seq.append((lvl, -1))
                else:
                    imb_seq.append((lvl, 0))
            elif a > 0:
                imb_seq.append((lvl, +1))
            elif b > 0:
                imb_seq.append((lvl, -1))
            else:
                imb_seq.append((lvl, 0))

        # Search consecutive same-sign sequences
        best_run = 0
        best_dir = 0
        run_len = 1
        run_dir = imb_seq[0][1] if imb_seq else 0

        for k in range(1, len(imb_seq)):
            if imb_seq[k][1] == run_dir and run_dir != 0:
                run_len += 1
            else:
                if run_len > best_run:
                    best_run = run_len
                    best_dir = run_dir
                run_len = 1
                run_dir = imb_seq[k][1]

        if run_len > best_run:
            best_run = run_len
            best_dir = run_dir

        if best_run >= min_stack and best_dir != 0:
            sig_indices.append(i)
            sig_name = "STACKED_IMB_BUY" if best_dir > 0 else "STACKED_IMB_SELL"
            sig_signals.append(sig_name)
            sig_scores.append(min(best_run / 6.0, 1.0))
            direction = "↑" if best_dir > 0 else "↓"
            sig_details.append(
                f"{direction} stack={best_run} levels "
                f"ratio≥{ratio_thresh}"
            )

    if not sig_indices:
        return pl.DataFrame()

    # Build the output DataFrame
    result = pl.DataFrame({
        "datetime_utc": [times[i] for i in sig_indices],
        "close":        [closes[i] for i in sig_indices],
        "volume":       [volumes[i] for i in sig_indices],
        "delta":        [deltas[i] for i in sig_indices],
        "signal":       sig_signals,
        "score":        sig_scores,
        "details":      sig_details,
    })

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 7 — EXHAUSTION / CLIMAX (Phase 3b)
# ═══════════════════════════════════════════════════════════════════════════════

EXHAUSTION_VOL_Z    = 2.5   # Minimum volume z-score
EXHAUSTION_MAX_MOVE = {"GC": 0.20, "NQ": 1.0}  # Price barely moves despite the volume
EXHAUSTION_WINDOW   = 30    # Measurement window

def detect_exhaustion(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    """
    Exhaustion / Climax — large volume but the price does not move.

    An institutional pushes hard but the market absorbs everything.
    The move is exhausted. A reversal often follows.

    Logic: on N ticks,
    - high volume z-score (the effort is there)
    - |price_change| very small (the result is not there)
    - reversal direction: inverse of delta (the pusher loses)
    """
    w = EXHAUSTION_WINDOW
    max_move = EXHAUSTION_MAX_MOVE.get(symbol, 0.5)

    df = df.with_columns([
        pl.col("volume").cast(pl.Float64).rolling_mean(WINDOW_SLOW).alias("vol_ma"),
        pl.col("volume").cast(pl.Float64).rolling_std(WINDOW_SLOW).alias("vol_std"),
        pl.col("close").cast(pl.Float64).alias("close_f"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ])

    df = df.with_columns([
        ((pl.col("volume").cast(pl.Float64) - pl.col("vol_ma")) /
         (pl.col("vol_std") + 1.0)).alias("vol_z"),
        (pl.col("close_f") - pl.col("close_f").shift(w)).abs().alias("price_range"),
        pl.col("delta_f").rolling_sum(w).alias("delta_cum"),
        pl.col("volume").cast(pl.Float64).rolling_sum(w).alias("vol_sum"),
    ])

    # Large volume + small move = exhaustion
    signals = df.filter(
        (pl.col("vol_z") >= EXHAUSTION_VOL_Z) &
        (pl.col("price_range") <= max_move) &
        (pl.col("vol_sum") > 0)
    )

    if signals.is_empty():
        return pl.DataFrame()

    # Direction: inverse of cumulative delta (the pusher loses → reversal)
    return signals.with_columns([
        pl.lit("EXHAUSTION").alias("signal"),
        (pl.col("vol_z") / 6.0).clip(0, 1).alias("score"),
        (pl.concat_str([
            pl.lit("WALL "),
            pl.when(pl.col("delta_cum") > 0).then(pl.lit("↓")).otherwise(pl.lit("↑")),
            pl.lit(" vol_z="), pl.col("vol_z").round(2).cast(pl.Utf8),
            pl.lit(" Δpx="), pl.col("price_range").round(2).cast(pl.Utf8),
            pl.lit(" vol_w="), pl.col("vol_sum").round(0).cast(pl.Utf8),
        ])).alias("details"),
    ])


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 8 — SESSION CONTEXT (Phase 3b)
# ═══════════════════════════════════════════════════════════════════════════════

IB_MINUTES = 60  # Initial Balance = first hour per session

def detect_session_context(df: pl.DataFrame, symbol: str) -> pl.DataFrame:
    """
    Phase 6 — Detect IB_BREAK and OPENING_DRIVE on 3 sessions:
    ASIA (23:00-07:00), EUROPE (07:00-13:30), US_RTH (13:30-20:00).

    Each session has its own IB and OD. The bullish false breakout (FADE↓)
    works on all 3 sessions (85-91% over 2.5 years, GC+NQ).

    Signal names: IB_BREAK_UP, IB_BREAK_DOWN, OPENING_DRIVE_UP/DOWN
    (identical regardless of session — the composite uses SIGNAL_EDGE
    to weight, and the detail contains the session name).
    """
    from datetime import timedelta

    if df.is_empty():
        return pl.DataFrame()

    # Get all unique dates
    dates = df.with_columns(
        pl.col("datetime_utc").dt.date().alias("_date")
    )["_date"].unique().sort().to_list()

    all_signals = []

    for date in dates:
        for sess_name, cfg in SESSION_IB_CONFIG.items():
            ib_start_h = cfg["ib_start_h"]
            ib_start_m = cfg["ib_start_m"]
            ib_minutes = cfg["ib_minutes"]
            od_minutes = cfg["od_minutes"]

            # Compute session start
            if sess_name == "ASIA":
                # Asia starts the day before at 23:00
                ib_start = datetime(date.year, date.month, date.day,
                                    ib_start_h, ib_start_m) - timedelta(days=1)
                sess_end = datetime(date.year, date.month, date.day, 7, 0)
            elif sess_name == "EUROPE":
                ib_start = datetime(date.year, date.month, date.day,
                                    ib_start_h, ib_start_m)
                sess_end = datetime(date.year, date.month, date.day, 13, 30)
            else:  # US_RTH
                ib_start = datetime(date.year, date.month, date.day,
                                    ib_start_h, ib_start_m)
                sess_end = datetime(date.year, date.month, date.day, 20, 0)

            ib_end = ib_start + timedelta(minutes=ib_minutes)
            od_end = ib_start + timedelta(minutes=od_minutes)

            # Filter ticks for this session
            session = df.filter(
                (pl.col("datetime_utc") >= ib_start) &
                (pl.col("datetime_utc") < sess_end)
            )
            if session.shape[0] < 100:
                continue

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
                    fade_dir = "↓" if od_move > 0 else "↑"
                    all_signals.append({
                        "datetime_utc": od["datetime_utc"][-1],
                        "close": od_close,
                        "volume": od_vol,
                        "delta": int(od["delta"].sum()),
                        "signal": f"OPENING_DRIVE_{direction}",
                        "score": min(abs(od_move) / (min_od * 3), 1.0),
                        "details": f"FADE{fade_dir} {sess_name} drive={direction} Δpx={od_move:+.2f}",
                    })

            # IB
            ib = session.filter(
                (pl.col("datetime_utc") >= ib_start) &
                (pl.col("datetime_utc") <= ib_end)
            )
            if ib.is_empty() or ib.shape[0] < 20:
                continue

            ib_high = float(ib["close"].max())
            ib_low = float(ib["close"].min())

            # Post-IB breaks
            post_ib = session.filter(pl.col("datetime_utc") > ib_end)
            if post_ib.is_empty():
                continue

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
                        "signal": "IB_BREAK_UP",
                        "score": 0.9,
                        "details": f"FADE↓ {sess_name} IB=[{ib_low:.2f}-{ib_high:.2f}] break={row['close']:.2f}",
                    })
                if not ib_broken_down and row["close"] < ib_low:
                    ib_broken_down = True
                    all_signals.append({
                        "datetime_utc": row["datetime_utc"],
                        "close": row["close"],
                        "volume": int(row["volume"]),
                        "delta": int(row["delta"]),
                        "signal": "IB_BREAK_DOWN",
                        "score": 0.7,
                        "details": f"FADE↑ {sess_name} IB=[{ib_low:.2f}-{ib_high:.2f}] break={row['close']:.2f}",
                    })
                if ib_broken_up and ib_broken_down:
                    break

    if not all_signals:
        return pl.DataFrame()

    return pl.DataFrame(all_signals)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — COMPOSITE SCORE
# ═══════════════════════════════════════════════════════════════════════════════

# Calibrated edge per signal and per symbol = (hit_rate_120s - 50) / 50
# 0.0 = no edge (50%), 0.50 = strong (75%), 0.87 = exceptional (93.5%)
# Direction already encoded in the signal (FADE/REVERSAL/MOMENTUM)

SIGNAL_EDGE = {
    # Phase 6 — Edges recalibrated on 3 sessions (ASIA/EUROPE/US_RTH)
    # Edge = (hit_rate_120s - 50) / 50. Value = best confirmed session.
    "GC": {
        "IB_BREAK_UP":       0.71,   # ASIA 85.5%, EUR 83.7%, RTH 78.1% → FADE↓
        "OPENING_DRIVE_UP":  0.41,   # ASIA 70.6%, EUR 64.1%, RTH 65.8% → FADE↓
        "IB_BREAK_DOWN":     0.33,   # ASIA 56.3%, EUR 66.3%, RTH 58.8% → FADE↑
        "DELTA_DIV_BULL":    0.18,   # RTH 54.1%, EUR 57.2% → REVERSAL↓
        "ABSORPTION_BULL":   0.13,   # RTH 51.5%, EUR 53.7% → REVERSAL↓
        "BURST":             0.01,   # ~50%  MOMENTUM
        "EXHAUSTION":        0.02,   # ~50%  INFO
        "ICEBERG":           0.00,   # ~50%  INFO
        "LARGE_PRINT":       0.00,   # ~50%  INFO
        "STACKED_IMB_SELL":  0.04,   # 51.8%  INFO
        "DELTA_DIV_BEAR":    0.00,   # → inverted, not reliable
        "ABSORPTION_BEAR":   0.00,   # → inverted, not reliable
        "OPENING_DRIVE_DOWN":0.00,   # sub-50% everywhere
        "STACKED_IMB_BUY":   0.00,   # → inverted
    },
    "NQ": {
        "IB_BREAK_UP":       0.82,   # ASIA 91.0%, EUR 89.4%, RTH 90.6% → FADE↓
        "OPENING_DRIVE_UP":  0.27,   # ASIA 60.7%, EUR 63.4%, RTH 58.1% → FADE↓
        "IB_BREAK_DOWN":     0.14,   # ASIA 52.2%, EUR 56.8%, RTH 50.8% → FADE↑
        "DELTA_DIV_BULL":    0.13,   # RTH 51.2%, vol≥20 56.3% → REVERSAL↓
        "ABSORPTION_BULL":   0.05,   # RTH 51.1%, vol≥20 52.5% → REVERSAL↓
        "OPENING_DRIVE_DOWN":0.00,   # sub-50% everywhere
        "DELTA_DIV_BEAR":    0.00,   # → not reliable
        "ABSORPTION_BEAR":   0.00,   # → not reliable
        "LARGE_PRINT":       0.00,   # ~49%
        "BURST":             0.00,   # ~48%
        "ICEBERG":           0.00,   # ~47%
        "EXHAUSTION":        0.00,   # ~48%
        "STACKED_IMB_BUY":   0.00,   # ~50%
        "STACKED_IMB_SELL":  0.00,   # ~49%
    },
}

# Session signals — persist throughout the RTH session
SESSION_SIGNALS = {"IB_BREAK_UP", "IB_BREAK_DOWN", "OPENING_DRIVE_UP", "OPENING_DRIVE_DOWN"}

# Flow signals — time windows
FLOW_SIGNALS = {"ABSORPTION_BULL", "ABSORPTION_BEAR", "DELTA_DIV_BULL", "DELTA_DIV_BEAR"}

# Flow aggregation windows (in minutes)
FLOW_WINDOWS = [5, 30, 120]  # 5 min, 30 min, 2 hours

# ── Phase 5 — NQ-specific optimizations ──
# H1 confirmed: Opening (13:30-14:30 UTC) gives +4pts edge on DDV_BULL NQ
# H4 confirmed: isolated BULL signals sub-50%, confluent +3pts. Isolated BEAR OK.
NQ_TIME_BOOST = {
    # (hour_start, hour_end): multiplier on the NQ flow edge
    (13, 14): 1.4,   # Opening: DDV_BULL 56.9% vs 53.2% baseline
    (14, 16): 1.0,   # EU_Close: normal
    (16, 18): 0.5,   # Midday: little edge, noisy signals
    (18, 19): 0.8,   # Pre-close: slight discount
    (19, 20): 1.2,   # PowerHr: ABS_BULL 52.1% but good volume
}

# NQ BULL signals that require confluence (isolated = sub-50%)
NQ_CONFLUENCE_REQUIRED = {"ABSORPTION_BULL", "DELTA_DIV_BULL"}


def _signal_direction(sig: str, row: dict) -> int:
    """Predicted direction of a signal. -1=bearish, +1=bullish, 0=neutral."""
    if sig in ("IB_BREAK_UP", "OPENING_DRIVE_UP",
               "ABSORPTION_BULL", "DELTA_DIV_BULL"):
        return -1   # FADE↓ or REVERSAL↓
    if sig in ("IB_BREAK_DOWN", "OPENING_DRIVE_DOWN",
               "ABSORPTION_BEAR", "DELTA_DIV_BEAR"):
        return +1   # FADE↑ or REVERSAL↑
    # BURST, LARGE_PRINT, etc. → direction of delta
    delta = row.get("delta", 0) or 0
    return 1 if delta > 0 else -1 if delta < 0 else 0


def compute_composite(signals: pl.DataFrame, symbol: str) -> dict:
    """
    3-layer composite score:

    1. SESSION — IB_BREAK, OPENING_DRIVE
       Persist throughout the session. Strong weight (calibrated edge).
       An IB_BREAK_UP today = bearish bias until close.

    2. FLOW    — ABSORPTION, DELTA_DIVERGENCE
       Time windows (5m, 30m, 2h). What flow is saying right now.

    3. PULSE   — BURST, LARGE_PRINT, ICEBERG, etc.
       Instantaneous energy. No reliable direction, but measures intensity.

    Final score = session_bias + flow_score
    Conviction  = layer alignment (do session and flow point the same way?)
    Energy      = average volume of recent signals
    """
    if signals.is_empty():
        return {}

    edges = SIGNAL_EDGE.get(symbol, SIGNAL_EDGE["GC"])
    latest_ts = signals["datetime_utc"].max()
    latest_date = latest_ts.date() if hasattr(latest_ts, 'date') else None

    # ── LAYER 1: SESSION ──
    # Find session signals from the most recent day
    session_sigs = signals.filter(
        pl.col("signal").is_in(list(SESSION_SIGNALS))
    )

    session_bias = 0.0
    session_details = []

    if not session_sigs.is_empty():
        # Take the latest session (latest date with session signals)
        session_sigs = session_sigs.with_columns(
            pl.col("datetime_utc").dt.date().alias("_date")
        )
        last_session_date = session_sigs["_date"].max()
        today_session = session_sigs.filter(pl.col("_date") == last_session_date)

        for row in today_session.iter_rows(named=True):
            sig = row["signal"]
            edge = edges.get(sig, 0.0)
            sig_dir = _signal_direction(sig, row)
            session_bias += edge * sig_dir
            if edge > 0.01:
                arrow = "↓" if sig_dir < 0 else "↑"
                session_details.append(f"{sig} {arrow} ({edge:.0%})")

    # ── LAYER 2: FLOW ──
    flow_results = {}
    for window_min in FLOW_WINDOWS:
        cutoff = latest_ts - pl.duration(minutes=window_min)
        recent = signals.filter(pl.col("datetime_utc") >= cutoff)

        if recent.is_empty():
            flow_results[window_min] = {
                "flow_score": 0.0, "n_flow": 0, "n_edge": 0,
                "energy": 0.0, "n_total": 0,
            }
            continue

        flow_score = 0.0
        n_flow = 0
        n_edge = 0
        edge_dirs = []

        # Phase 5: precompute NQ confluence (group signals by minute+direction)
        confluence_set = set()
        if symbol == "NQ" and not recent.is_empty():
            # Build a set of (minute_bucket, direction) having 2+ distinct signals
            _sig_by_min = {}  # minute_bucket → set of (signal_name, direction)
            for _r in recent.iter_rows(named=True):
                _s = _r["signal"]
                if _s in SESSION_SIGNALS:
                    continue
                _ts = _r["datetime_utc"]
                _min_bucket = _ts.hour * 60 + _ts.minute
                _d = _signal_direction(_s, _r)
                if _min_bucket not in _sig_by_min:
                    _sig_by_min[_min_bucket] = set()
                _sig_by_min[_min_bucket].add((_s, _d))
            # A bucket is "confluent" if ≥2 DIFFERENT signals point in the same direction
            for _mb, _sigs in _sig_by_min.items():
                for _target_dir in (-1, 1):
                    names = {s for s, d in _sigs if d == _target_dir}
                    if len(names) >= 2:
                        confluence_set.add((_mb, _target_dir))

        for row in recent.iter_rows(named=True):
            sig = row["signal"]
            if sig in SESSION_SIGNALS:
                continue  # already counted in the session layer
            edge = edges.get(sig, 0.0)
            sig_dir = _signal_direction(sig, row)

            # Phase 5 — NQ time-of-day boost
            if symbol == "NQ" and sig in FLOW_SIGNALS:
                h = row["datetime_utc"].hour
                boost = 1.0
                for (h_start, h_end), mult in NQ_TIME_BOOST.items():
                    if h_start <= h < h_end:
                        boost = mult
                        break
                edge = edge * boost

            # Phase 5 — NQ confluence gate for BULL signals
            if symbol == "NQ" and sig in NQ_CONFLUENCE_REQUIRED:
                ts = row["datetime_utc"]
                min_bucket = ts.hour * 60 + ts.minute
                if (min_bucket, sig_dir) not in confluence_set:
                    edge = 0.0  # Isolated BULL signal on NQ = no edge

            flow_score += edge * sig_dir
            if sig in FLOW_SIGNALS and edge > 0.01:
                n_flow += 1
                edge_dirs.append(sig_dir)
            if edge > 0.05:
                n_edge += 1

        # Energy = average volume of all recent signals
        vol_sum = recent["volume"].sum()
        n_total = recent.shape[0]
        energy = vol_sum / max(n_total, 1)

        flow_results[window_min] = {
            "flow_score": round(flow_score, 3),
            "n_flow": n_flow,
            "n_edge": n_edge,
            "energy": round(energy, 0),
            "n_total": n_total,
            "edge_dirs": edge_dirs,
        }

    # ── ASSEMBLY ──
    results = {
        "session_bias": round(session_bias, 3),
        "session_details": session_details,
        "windows": {},
    }

    for window_min in FLOW_WINDOWS:
        fr = flow_results[window_min]
        total_dir = session_bias + fr["flow_score"]

        # Conviction = do session and flow align?
        edge_dirs = fr.get("edge_dirs", [])
        all_dirs = edge_dirs.copy()
        if abs(session_bias) > 0.01:
            # Add the session vote (weight = number of session signals)
            session_vote = 1 if session_bias > 0 else -1
            all_dirs.append(session_vote)

        if len(all_dirs) >= 2:
            majority_dir = 1 if total_dir > 0 else -1
            n_agree = sum(1 for d in all_dirs if d == majority_dir)
            conviction = n_agree / len(all_dirs)
        elif len(all_dirs) == 1:
            conviction = 1.0
        else:
            conviction = 0.0

        # Label
        if abs(total_dir) < 0.05:
            label = "NEUTRAL"
        elif total_dir > 0:
            label = f"BULL +{total_dir:.2f}"
        else:
            label = f"BEAR {total_dir:.2f}"

        results["windows"][window_min] = {
            "direction": round(total_dir, 3),
            "flow_score": fr["flow_score"],
            "conviction": round(conviction, 2),
            "energy": fr["energy"],
            "n_total": fr["n_total"],
            "n_flow": fr["n_flow"],
            "n_edge": fr["n_edge"],
            "label": label,
        }

    return results


def print_composite(results: dict, symbol: str):
    """Print the 3-layer composite score in the terminal."""
    if not results:
        return

    session_bias = results.get("session_bias", 0.0)
    session_details = results.get("session_details", [])
    windows = results.get("windows", {})

    print(f"\n  {BOLD}{'═' * 60}{RESET}")
    print(f"  {BOLD}{CYAN}  COMPOSITE — {symbol}{RESET}")
    print(f"  {'═' * 60}")

    # ── Session layer ──
    if session_details:
        s_color = GREEN if session_bias > 0 else RED if session_bias < 0 else ""
        s_rst = RESET if s_color else ""
        s_arrow = "▲" if session_bias > 0 else "▼" if session_bias < 0 else "●"
        print(f"\n  {BOLD}SESSION{RESET}  {s_color}{s_arrow} bias={session_bias:+.2f}{s_rst}")
        for detail in session_details:
            print(f"    {detail}")
    else:
        print(f"\n  {BOLD}SESSION{RESET}  {YELLOW}● no session signal{RESET}")

    # ── Flow + Composite ──
    print(f"\n  {BOLD}FLOW + COMPOSITE{RESET}")
    print(f"  {'Window':>8s}  {'Session':>8s}  {'Flow':>8s}  {'= Total':>10s}  "
          f"{'Conviction':>10s}  {'Signals':>8s}  {'Energy':>8s}")
    print(f"  {'─' * 60}")

    for window_min in FLOW_WINDOWS:
        w = windows.get(window_min, {})
        if not w:
            continue

        d = w["direction"]
        fs = w["flow_score"]
        c = w["conviction"]
        n = w["n_total"]
        nf = w["n_flow"]
        e = w["energy"]
        label = w["label"]

        # Color
        if "BULL" in label:
            color = GREEN
        elif "BEAR" in label:
            color = RED
        else:
            color = ""
        rst = RESET if color else ""

        # Conviction bar
        n_bars = int(c * 10)
        bar = "█" * n_bars + "░" * (10 - n_bars)

        w_label = f"{window_min}m" if window_min < 60 else f"{window_min // 60}h"

        print(f"  {w_label:>8s}  {session_bias:>+8.2f}  {fs:>+8.2f}  "
              f"{color}{d:>+10.2f}{rst}  {bar}  {nf:>3}/{n:<4}  {e:>7,.0f}")

    # ── Verdict ──
    w30 = windows.get(30, {})
    if w30:
        d = w30["direction"]
        c = w30["conviction"]
        nf = w30["n_flow"]
        has_session = abs(session_bias) > 0.01
        has_flow = nf >= 1

        if (has_session or has_flow) and abs(d) >= 0.05:
            v_color = GREEN if d > 0 else RED
            v_arrow = "▲" if d > 0 else "▼"
            v_word = "BULLISH" if d > 0 else "BEARISH"

            # Verdict strength
            if abs(d) >= 0.50 and c >= 0.80:
                strength = "STRONG"
            elif abs(d) >= 0.20 and c >= 0.60:
                strength = "MODERATE"
            else:
                strength = "WEAK"

            # Sources
            sources = []
            if has_session:
                sources.append("session")
            if has_flow:
                sources.append("flow")
            src = " + ".join(sources)

            print(f"\n  {v_color}{BOLD}  ➤ {v_arrow} {v_word} — {strength} "
                  f"(dir={d:+.2f} conv={c:.0%} via {src}){RESET}")
        elif abs(d) < 0.05:
            print(f"\n  {YELLOW}{BOLD}  ➤ ● NEUTRAL — forces in balance "
                  f"(dir={d:+.2f}){RESET}")
        else:
            print(f"\n  {YELLOW}  ➤ ● WEAK SIGNAL — direction={d:+.2f} "
                  f"conv={c:.0%}{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORT COLUMNS
# ═══════════════════════════════════════════════════════════════════════════════

OUTPUT_COLS = [
    "datetime_utc", "contract", "close", "volume", "bid_vol", "ask_vol",
    "delta", "signal", "score", "details",
]


def extract_output(df: pl.DataFrame) -> pl.DataFrame:
    """Keep only the available output columns."""
    available = [c for c in OUTPUT_COLS if c in df.columns]
    return df.select(available)


# ═══════════════════════════════════════════════════════════════════════════════
# PER-SYMBOL ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_symbol(symbol: str, days: int = None) -> Optional[pl.DataFrame]:
    print(f"\n  {CYAN}{'─' * 50}{RESET}")
    print(f"  {BOLD}{symbol}{RESET}")
    print(f"  {'─' * 50}")

    df = load_ticks(symbol, days=days)
    if df.is_empty():
        return None

    n_raw = df.shape[0]
    min_vol = MIN_VOLUME.get(symbol, 5)
    df = df.filter(pl.col("volume") >= min_vol)
    n = df.shape[0]
    oldest = df["datetime_utc"][0]
    newest = df["datetime_utc"][-1]
    print(f"  {n_raw:,} raw ticks → {n:,} after vol≥{min_vol} filter ({n/n_raw*100:.1f}%)")
    print(f"  {oldest.strftime('%Y-%m-%d')} → {newest.strftime('%Y-%m-%d')}")

    tick = TICK_SIZE[symbol]
    all_signals = []

    # 1. Large Prints
    print(f"  Large Prints...", end=" ", flush=True)
    lp = detect_large_prints(df)
    lp = extract_output(lp)
    print(f"{lp.shape[0]:,} signals")
    all_signals.append(lp)

    # 2. Absorption
    print(f"  Absorption...", end=" ", flush=True)
    ab = detect_absorption(df)
    ab = extract_output(ab)
    print(f"{ab.shape[0]:,} signals")
    all_signals.append(ab)

    # 3. Iceberg
    print(f"  Iceberg...", end=" ", flush=True)
    ic = detect_iceberg(df, tick)
    ic = extract_output(ic)
    print(f"{ic.shape[0]:,} signals")
    all_signals.append(ic)

    # 4. Burst
    print(f"  Burst...", end=" ", flush=True)
    bu = detect_burst(df)
    bu = extract_output(bu)
    print(f"{bu.shape[0]:,} signals")
    all_signals.append(bu)

    # 5. Delta Divergence (Phase 3)
    print(f"  Delta Divergence...", end=" ", flush=True)
    dd = detect_delta_divergence(df, symbol)
    dd = extract_output(dd)
    print(f"{dd.shape[0]:,} signals")
    all_signals.append(dd)

    # 6. Stacked Imbalance (Phase 3)
    print(f"  Stacked Imbalance...", end=" ", flush=True)
    si = detect_stacked_imbalance(df, tick)
    si = extract_output(si)
    print(f"{si.shape[0]:,} signals")
    all_signals.append(si)

    # 7. Exhaustion / Climax (Phase 3b)
    print(f"  Exhaustion...", end=" ", flush=True)
    ex = detect_exhaustion(df, symbol)
    ex = extract_output(ex)
    print(f"{ex.shape[0]:,} signals")
    all_signals.append(ex)

    # 8. Session Context (Phase 3b)
    print(f"  Session Context...", end=" ", flush=True)
    sc = detect_session_context(df, symbol)
    sc = extract_output(sc)
    print(f"{sc.shape[0]:,} signals")
    all_signals.append(sc)

    # Combine + sort (diagonal_relaxed tolerates missing columns)
    combined = pl.concat([s for s in all_signals if not s.is_empty()], how="diagonal_relaxed")
    combined = combined.sort("datetime_utc")

    # Stats — ranked by calibrated reliability
    total = combined.shape[0]
    print(f"\n  {BOLD}Total: {total:,} institutional signals{RESET}")

    # Calibrated reliability (Phase 2+3+3b — 650 sessions GC+NQ)
    CALIBRATED = {
        "IB_BREAK_UP":       ("★★★★","86-94%", "FADE↓"),
        "DELTA_DIV_BULL":    ("★★★", "59-64%", "REVERSAL↓"),
        "IB_BREAK_DOWN":     ("★★★", "64%GC",  "FADE↑"),
        "OPENING_DRIVE_UP":  ("★★★", "62%",    "FADE↓"),
        "ABSORPTION_BULL":   ("★★★", "56-61%", "REVERSAL↓"),
        "BURST":             ("★★★", "~67%",   "MOMENTUM"),
        "DELTA_DIV_BEAR":    ("★★",  "54%GC",  "REVERSAL↑"),
        "ABSORPTION_BEAR":   ("★★",  "50-51%", "REVERSAL↑"),
        "ICEBERG":           ("★★",  "~55%",   "INFO"),
        "EXHAUSTION":        ("★",   "~50%",   "INFO"),
        "OPENING_DRIVE_DOWN":("★",   "~51%",   "INFO"),
        "LARGE_PRINT":       ("★",   "~50%",   "INFO"),
        "STACKED_IMB_BUY":   ("★",   "<50%",   "INFO"),
        "STACKED_IMB_SELL":  ("★",   "<50%",   "INFO"),
    }

    counts = (
        combined.group_by("signal")
        .agg(
            pl.len().alias("count"),
            pl.col("score").mean().alias("avg_score"),
        )
        .sort("count", descending=True)
    )
    for row in counts.iter_rows(named=True):
        sig = row["signal"]
        pct = row["count"] / total * 100
        cal = CALIBRATED.get(sig, ("?", "?", "?"))
        color = GREEN if "★★★" in cal[0] else YELLOW if cal[0] == "★★" else ""
        rst = RESET if color else ""
        print(f"    {color}{sig:20s}{rst}  {row['count']:6,} ({pct:5.1f}%)  "
              f"score={row['avg_score']:.2f}  {cal[0]} {cal[1]} {cal[2]}")

    # Save
    out = output_path(symbol)
    combined.write_parquet(out, compression="zstd")
    size = out.stat().st_size / 1e6
    print(f"\n  {GREEN}→ {out.name} — {size:.1f} MB{RESET}")

    # Phase 4 — Composite score
    composite = compute_composite(combined, symbol)
    print_composite(composite, symbol)

    return combined


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    # Args
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
    print(f"  {BOLD}{CYAN}  PULSE — Institutional Flow + Composite Score{RESET}")
    print(f"  {BOLD}{'═' * 55}{RESET}")
    if days:
        print(f"  Window: {days} most recent days")
    print(f"  Symbols: {', '.join(symbols)}")
    print()
    print(f"  Calibrated signals (650 sessions, 2.5 years):")
    print(f"    {GREEN}{BOLD}IB_BREAK_UP      — bullish false breakout FADE↓     (86-94%) ★★★★{RESET}")
    print(f"    {GREEN}IB_BREAK_DOWN    — bearish false breakout FADE↑     (64%GC)  ★★★{RESET}")
    print(f"    {GREEN}OPENING_DRIVE_UP — morning drive exhausts FADE↓     (62%)    ★★★{RESET}")
    print(f"    {GREEN}DELTA_DIV_BULL   — macro divergence REVERSAL↓       (59-64%) ★★★{RESET}")
    print(f"    {GREEN}ABSORPTION_BULL  — micro absorption REVERSAL↓       (56-61%) ★★★{RESET}")
    print(f"    {YELLOW}DELTA_DIV_BEAR   — macro divergence REVERSAL↑       (54%GC)  ★★{RESET}")
    print(f"    {YELLOW}ABSORPTION_BEAR  — micro absorption REVERSAL↑       (50-51%) ★★{RESET}")
    print(f"    ICEBERG, LARGE_PRINT, BURST, EXHAUSTION, STACKED_IMB       INFO     ★")

    for sym in symbols:
        analyze_symbol(sym, days=days)

    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {GREEN}Done.{RESET}")
    print()


if __name__ == "__main__":
    main()
