#!/usr/bin/env python3
"""
pulse_calibrate.py — Institutional threshold calibration (Phase 2)

Usage:
    python3 Scripts/pulse_calibrate.py
    python3 Scripts/pulse_calibrate.py --symbol GC
    python3 Scripts/pulse_calibrate.py --sample 30       (30 random days)
    python3 Scripts/pulse_calibrate.py --grid             (threshold grid search)

Source  : Data/Ticks_Parquet_Training/{symbol}_ticks.parquet
Produces: Data/Intel_Data/{symbol}_Calibration.parquet  (results per signal)

For each detected signal, measure whether the price moved in the predicted
direction within the next 30, 60 and 120 seconds.

Hit Rate = % of signals where the prediction was correct.
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

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR      = Path(__file__).parent.parent
DATA_DIR      = BASE_DIR / "Data"
TRAINING_DIR  = DATA_DIR / "Ticks_Parquet_Training"
INTEL_DIR     = DATA_DIR / "Intel_Data"
INTEL_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = ["GC", "NQ"]

TICK_SIZE   = {"GC": 0.10, "NQ": 0.25}
MIN_VOLUME  = {"GC": 5, "NQ": 10}
NAMES       = {"GC": "Gold COMEX", "NQ": "Nasdaq 100"}

# RTH sessions (UTC)
RTH = {
    "GC": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0},
    "NQ": {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0},
}

# Measurement horizons (seconds)
HORIZONS = [30, 60, 120]

# Default thresholds (identical to pulse_institutional.py)
DEFAULT_THRESHOLDS = {
    "large_print_z":     3.0,
    "burst_vol_z":       2.5,
    "burst_speed_z":     2.0,
    "iceberg_window":    20,
    "iceberg_min_hits":  4,
    "absorption_window": 30,
    "window_slow":       200,
    # min_volume is injected per symbol in calibrate_session
}

# Search grid (--grid mode)
GRID = {
    "large_print_z":     [2.0, 2.5, 3.0, 3.5, 4.0],
    "burst_vol_z":       [2.0, 2.5, 3.0],
    "burst_speed_z":     [1.5, 2.0, 2.5],
    "iceberg_min_hits":  [3, 4, 5, 6],
    "absorption_window": [20, 30, 40],
}

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[90m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# TRAINING DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_training(symbol: str) -> pl.DataFrame:
    """
    Load training data and normalize the schema.
    Training data has: num_trades, contract with exchange suffix (GCG24-COMEX).
    Normalize to the standard Pulse schema.
    """
    p = TRAINING_DIR / f"{symbol}_ticks.parquet"
    if not p.exists():
        print(f"  {RED}[{symbol}] Training data not found: {p}{RESET}")
        return pl.DataFrame()

    print(f"  Loading {p.name}...", end=" ", flush=True)
    t0 = time.time()
    df = pl.read_parquet(p)
    dt = time.time() - t0
    print(f"{df.shape[0]:,} ticks in {dt:.1f}s")

    # Normalize contract: "GCG24-COMEX" → "GCG24"
    if "contract" in df.columns:
        df = df.with_columns(
            pl.col("contract").str.split("-").list.first().alias("contract")
        )

    # Drop num_trades if present
    if "num_trades" in df.columns:
        df = df.drop("num_trades")

    # Ensure delta exists
    if "delta" not in df.columns and "bid_vol" in df.columns and "ask_vol" in df.columns:
        df = df.with_columns(
            (pl.col("ask_vol") - pl.col("bid_vol")).alias("delta")
        )

    return df


def get_rth_dates(df: pl.DataFrame, sym: str) -> list:
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
# DETECTION (PARAMETERIZABLE)
# ═══════════════════════════════════════════════════════════════════════════════

def detect_large_prints(df: pl.DataFrame, thresholds: dict) -> pl.DataFrame:
    z_thresh = thresholds["large_print_z"]
    w = thresholds["window_slow"]

    df2 = df.with_columns([
        pl.col("volume").cast(pl.Float64).rolling_mean(w).alias("vol_ma"),
        pl.col("volume").cast(pl.Float64).rolling_std(w).alias("vol_std"),
    ]).with_columns([
        ((pl.col("volume").cast(pl.Float64) - pl.col("vol_ma")) /
         (pl.col("vol_std") + 1.0)).alias("vol_z"),
    ])

    signals = df2.filter(pl.col("vol_z") >= z_thresh)
    if signals.is_empty():
        return pl.DataFrame()

    return signals.select([
        "datetime_utc", "close", "volume", "delta",
        pl.lit("LARGE_PRINT").alias("signal"),
        pl.col("vol_z").alias("z_score"),
        # Predicted direction: sign of delta
        pl.when(pl.col("delta") > 0).then(1)
          .when(pl.col("delta") < 0).then(-1)
          .otherwise(0).alias("predicted_dir"),
    ])


def detect_absorption(df: pl.DataFrame, thresholds: dict) -> pl.DataFrame:
    w = thresholds["absorption_window"]

    df2 = df.with_columns([
        pl.col("close").cast(pl.Float64).alias("close_f"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ]).with_columns([
        (pl.col("close_f") - pl.col("close_f").shift(w)).alias("price_change"),
        pl.col("delta_f").rolling_sum(w).alias("delta_cum"),
        pl.col("volume").cast(pl.Float64).rolling_mean(w).alias("vol_ma_abs"),
    ])

    vol_mean = df["volume"].cast(pl.Float64).mean() * 0.5

    # Bull absorption: price rises, negative delta → bearish REVERSAL
    # Passive seller absorbs aggressive buyers, the move exhausts
    bull = df2.filter(
        (pl.col("price_change") > 0) &
        (pl.col("delta_cum") < 0) &
        (pl.col("vol_ma_abs") > vol_mean)
    ).select([
        "datetime_utc", "close", "volume", "delta",
        pl.lit("ABSORPTION_BULL").alias("signal"),
        pl.col("price_change").alias("z_score"),
        pl.lit(-1).alias("predicted_dir"),
    ])

    # Bear absorption: price falls, positive delta → bullish REVERSAL
    # Passive buyer absorbs aggressive sellers, the move exhausts
    bear = df2.filter(
        (pl.col("price_change") < 0) &
        (pl.col("delta_cum") > 0) &
        (pl.col("vol_ma_abs") > vol_mean)
    ).select([
        "datetime_utc", "close", "volume", "delta",
        pl.lit("ABSORPTION_BEAR").alias("signal"),
        pl.col("price_change").alias("z_score"),
        pl.lit(1).alias("predicted_dir"),
    ])

    parts = [p for p in [bull, bear] if not p.is_empty()]
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts)


def detect_iceberg(df: pl.DataFrame, tick_size: float, thresholds: dict) -> pl.DataFrame:
    w = thresholds["iceberg_window"]
    min_hits = thresholds["iceberg_min_hits"]
    w_slow = thresholds["window_slow"]

    df2 = df.with_columns([
        (pl.col("close") / tick_size).round(0).cast(pl.Int64).alias("price_level"),
        pl.col("volume").cast(pl.Float64).rolling_mean(w_slow).alias("vol_ma_ice"),
    ])

    # Count hits at the same level within the window
    hit_cols = []
    for i in range(1, w + 1):
        hit_cols.append(
            (pl.col("price_level") == pl.col("price_level").shift(i))
            .cast(pl.Int32).alias(f"_h{i}")
        )

    df2 = df2.with_columns(hit_cols)
    hit_sum = sum(pl.col(f"_h{i}") for i in range(1, w + 1))
    df2 = df2.with_columns(hit_sum.alias("level_hits"))
    drop = [f"_h{i}" for i in range(1, w + 1)]
    df2 = df2.drop([c for c in drop if c in df2.columns])

    signals = df2.filter(
        (pl.col("level_hits") >= min_hits) &
        (pl.col("volume").cast(pl.Float64) >= pl.col("vol_ma_ice") * 0.8)
    )

    if signals.is_empty():
        return pl.DataFrame()

    # Direction: the iceberg accumulates in the direction of delta
    # delta > 0 = hidden buyer accumulating → price rises
    # delta < 0 = hidden seller accumulating → price falls
    return signals.select([
        "datetime_utc", "close", "volume", "delta",
        pl.lit("ICEBERG").alias("signal"),
        pl.col("level_hits").cast(pl.Float64).alias("z_score"),
        pl.when(pl.col("delta") > 0).then(1)
          .when(pl.col("delta") < 0).then(-1)
          .otherwise(0).alias("predicted_dir"),
    ])


def detect_burst(df: pl.DataFrame, thresholds: dict) -> pl.DataFrame:
    vol_z_thresh = thresholds["burst_vol_z"]
    speed_z_thresh = thresholds["burst_speed_z"]
    w_slow = thresholds["window_slow"]

    df2 = df.with_columns([
        pl.col("datetime_utc").cast(pl.Int64).alias("ts_us"),
        pl.col("volume").cast(pl.Float64).alias("vol_f"),
    ]).with_columns([
        ((pl.col("ts_us") - pl.col("ts_us").shift(1)).cast(pl.Float64) / 1_000_000.0)
        .alias("dt_sec"),
    ]).with_columns([
        (1.0 / (pl.col("dt_sec").clip(0.001, None))).alias("tick_speed"),
    ]).with_columns([
        pl.col("vol_f").rolling_mean(w_slow).alias("vol_ma_b"),
        pl.col("vol_f").rolling_std(w_slow).alias("vol_std_b"),
        pl.col("tick_speed").rolling_mean(w_slow).alias("speed_ma"),
        pl.col("tick_speed").rolling_std(w_slow).alias("speed_std"),
    ]).with_columns([
        ((pl.col("vol_f") - pl.col("vol_ma_b")) / (pl.col("vol_std_b") + 1.0)).alias("vol_z_b"),
        ((pl.col("tick_speed") - pl.col("speed_ma")) / (pl.col("speed_std") + 0.01)).alias("speed_z"),
    ])

    # Burst direction: sign of delta
    signals = df2.filter(
        (pl.col("vol_z_b") >= vol_z_thresh) &
        (pl.col("speed_z") >= speed_z_thresh)
    )

    if signals.is_empty():
        return pl.DataFrame()

    return signals.select([
        "datetime_utc", "close", "volume", "delta",
        pl.lit("BURST").alias("signal"),
        (pl.col("vol_z_b") + pl.col("speed_z")).alias("z_score"),
        pl.when(pl.col("delta") > 0).then(1)
          .when(pl.col("delta") < 0).then(-1)
          .otherwise(0).alias("predicted_dir"),
    ])


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 5 — DELTA DIVERGENCE (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

DELTA_DIV_WINDOW   = 200
DELTA_DIV_MIN_MOVE = {"GC": 1.0, "NQ": 5.0}

def detect_delta_divergence(df: pl.DataFrame, symbol: str, thresholds: dict) -> pl.DataFrame:
    w = thresholds.get("delta_div_window", DELTA_DIV_WINDOW)
    min_move = DELTA_DIV_MIN_MOVE.get(symbol, 1.0)

    df2 = df.with_columns([
        pl.col("close").cast(pl.Float64).alias("close_f"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ]).with_columns([
        (pl.col("close_f") - pl.col("close_f").shift(w)).alias("price_move"),
        pl.col("delta_f").rolling_sum(w).alias("delta_cum"),
    ])

    # Bull divergence: price↑ but delta_cum↓ → REVERSAL DOWN
    div_bull = df2.filter(
        (pl.col("price_move") >= min_move) &
        (pl.col("delta_cum") < 0)
    )
    if not div_bull.is_empty():
        div_bull = div_bull.select([
            "datetime_utc", "close", "volume", "delta",
            pl.lit("DELTA_DIV_BULL").alias("signal"),
            pl.col("price_move").abs().alias("z_score"),
            pl.lit(-1).alias("predicted_dir"),
        ])

    # Bear divergence: price↓ but delta_cum↑ → REVERSAL UP
    div_bear = df2.filter(
        (pl.col("price_move") <= -min_move) &
        (pl.col("delta_cum") > 0)
    )
    if not div_bear.is_empty():
        div_bear = div_bear.select([
            "datetime_utc", "close", "volume", "delta",
            pl.lit("DELTA_DIV_BEAR").alias("signal"),
            pl.col("price_move").abs().alias("z_score"),
            pl.lit(1).alias("predicted_dir"),
        ])

    parts = [p for p in [div_bull, div_bear] if not p.is_empty()]
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 6 — STACKED IMBALANCE (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

IMBALANCE_RATIO     = 3.0
IMBALANCE_STACK_MIN = 3
IMBALANCE_WINDOW    = 50

def detect_stacked_imbalance(df: pl.DataFrame, tick_size: float, thresholds: dict) -> pl.DataFrame:
    w = thresholds.get("imbalance_window", IMBALANCE_WINDOW)
    min_stack = thresholds.get("imbalance_stack_min", IMBALANCE_STACK_MIN)
    ratio_thresh = thresholds.get("imbalance_ratio", IMBALANCE_RATIO)

    df2 = df.with_columns([
        (pl.col("close") / tick_size).round(0).cast(pl.Int64).alias("price_level"),
    ])

    prices = df2["price_level"].to_list()
    bid_vols = df2["bid_vol"].to_list()
    ask_vols = df2["ask_vol"].to_list()
    times = df2["datetime_utc"].to_list()
    closes = df2["close"].to_list()
    volumes = df2["volume"].to_list()
    deltas = df2["delta"].to_list()

    n = len(prices)
    sig_data = []

    for i in range(w, n, w // 2):
        level_bid = {}
        level_ask = {}
        for j in range(max(0, i - w), i + 1):
            lvl = prices[j]
            level_bid[lvl] = level_bid.get(lvl, 0) + bid_vols[j]
            level_ask[lvl] = level_ask.get(lvl, 0) + ask_vols[j]

        sorted_levels = sorted(level_bid.keys())
        if len(sorted_levels) < min_stack:
            continue

        imb_seq = []
        for lvl in sorted_levels:
            b = level_bid.get(lvl, 0)
            a = level_ask.get(lvl, 0)
            if a > 0 and b > 0:
                if a / b >= ratio_thresh:
                    imb_seq.append(+1)
                elif b / a >= ratio_thresh:
                    imb_seq.append(-1)
                else:
                    imb_seq.append(0)
            elif a > 0:
                imb_seq.append(+1)
            elif b > 0:
                imb_seq.append(-1)
            else:
                imb_seq.append(0)

        best_run = 0
        best_dir = 0
        run_len = 1
        run_dir = imb_seq[0] if imb_seq else 0
        for k in range(1, len(imb_seq)):
            if imb_seq[k] == run_dir and run_dir != 0:
                run_len += 1
            else:
                if run_len > best_run:
                    best_run = run_len
                    best_dir = run_dir
                run_len = 1
                run_dir = imb_seq[k]
        if run_len > best_run:
            best_run = run_len
            best_dir = run_dir

        if best_run >= min_stack and best_dir != 0:
            sig_data.append({
                "datetime_utc": times[i],
                "close": closes[i],
                "volume": volumes[i],
                "delta": deltas[i],
                "signal": "STACKED_IMB_BUY" if best_dir > 0 else "STACKED_IMB_SELL",
                "z_score": float(best_run),
                "predicted_dir": best_dir,
            })

    if not sig_data:
        return pl.DataFrame()

    return pl.DataFrame(sig_data)


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 7 — EXHAUSTION / CLIMAX (Phase 3b)
# ═══════════════════════════════════════════════════════════════════════════════

EXHAUSTION_VOL_Z    = 2.5
EXHAUSTION_MAX_MOVE = {"GC": 0.20, "NQ": 1.0}
EXHAUSTION_WINDOW   = 30

def detect_exhaustion(df: pl.DataFrame, symbol: str, thresholds: dict) -> pl.DataFrame:
    w = thresholds.get("exhaustion_window", EXHAUSTION_WINDOW)
    vol_z_thresh = thresholds.get("exhaustion_vol_z", EXHAUSTION_VOL_Z)
    max_move = EXHAUSTION_MAX_MOVE.get(symbol, 0.5)
    w_slow = thresholds.get("window_slow", 200)

    df2 = df.with_columns([
        pl.col("volume").cast(pl.Float64).rolling_mean(w_slow).alias("vol_ma"),
        pl.col("volume").cast(pl.Float64).rolling_std(w_slow).alias("vol_std"),
        pl.col("close").cast(pl.Float64).alias("close_f"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ]).with_columns([
        ((pl.col("volume").cast(pl.Float64) - pl.col("vol_ma")) /
         (pl.col("vol_std") + 1.0)).alias("vol_z"),
        (pl.col("close_f") - pl.col("close_f").shift(w)).abs().alias("price_range"),
        pl.col("delta_f").rolling_sum(w).alias("delta_cum"),
    ])

    signals = df2.filter(
        (pl.col("vol_z") >= vol_z_thresh) &
        (pl.col("price_range") <= max_move)
    )

    if signals.is_empty():
        return pl.DataFrame()

    # Direction: inverse of cumulative delta (the pusher loses → reversal)
    return signals.select([
        "datetime_utc", "close", "volume", "delta",
        pl.lit("EXHAUSTION").alias("signal"),
        pl.col("vol_z").alias("z_score"),
        pl.when(pl.col("delta_cum") > 0).then(-1)
          .when(pl.col("delta_cum") < 0).then(1)
          .otherwise(0).alias("predicted_dir"),
    ])


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 8 — SESSION CONTEXT (Phase 3b)
# ═══════════════════════════════════════════════════════════════════════════════

IB_MINUTES = 60

def detect_session_context(all_ticks: pl.DataFrame, symbol: str, thresholds: dict) -> pl.DataFrame:
    """
    IB Break and Opening Drive — session signals.
    Takes the FULL session ticks (unfiltered by volume).
    IB_BREAK: continuation (break → price continues in the same direction)
    OPENING_DRIVE: continuation (30 min direction → price continues)
    """
    from datetime import timedelta

    rth = RTH.get(symbol, {"open_h": 13, "open_m": 30, "close_h": 20, "close_m": 0})
    max_move_thresh = EXHAUSTION_MAX_MOVE.get(symbol, 0.5) * 5

    df = all_ticks.with_columns([
        pl.col("datetime_utc").dt.hour().alias("hour"),
    ]).filter(
        (pl.col("hour") >= rth["open_h"]) &
        (pl.col("hour") < rth["close_h"])
    )

    if df.is_empty() or df.shape[0] < 100:
        return pl.DataFrame()

    session_start = df["datetime_utc"][0]

    sig_data = []

    # Opening drive: first 30 minutes
    od_end = session_start + timedelta(minutes=30)
    od = df.filter(pl.col("datetime_utc") <= od_end)
    if od.shape[0] >= 10:
        od_open = od["close"][0]
        od_close = od["close"][-1]
        od_move = od_close - od_open
        od_vol = od["volume"].sum()

        if abs(od_move) >= max_move_thresh:
            # FADE: the opening drive exhausts → reversal
            # OD_UP → predicted -1 (fade down), OD_DOWN → predicted +1 (fade up)
            raw_dir = 1 if od_move > 0 else -1
            sig_name = "OPENING_DRIVE_UP" if raw_dir > 0 else "OPENING_DRIVE_DOWN"
            sig_data.append({
                "datetime_utc": od["datetime_utc"][-1],
                "close": od_close,
                "volume": int(od_vol),
                "delta": int(od["delta"].sum()),
                "signal": sig_name,
                "z_score": abs(od_move),
                "predicted_dir": -raw_dir,  # FADE — inverse of direction
            })

    # IB: first hour
    ib_end = session_start + timedelta(minutes=IB_MINUTES)
    ib = df.filter(pl.col("datetime_utc") <= ib_end)
    if ib.shape[0] >= 20:
        ib_high = ib["close"].max()
        ib_low = ib["close"].min()

        # Post-IB: look for breaks
        post_ib = df.filter(pl.col("datetime_utc") > ib_end)
        ib_broken_up = False
        ib_broken_down = False

        for row in post_ib.iter_rows(named=True):
            if not ib_broken_up and row["close"] > ib_high:
                ib_broken_up = True
                sig_data.append({
                    "datetime_utc": row["datetime_utc"],
                    "close": row["close"],
                    "volume": int(row["volume"]),
                    "delta": int(row["delta"]),
                    "signal": "IB_BREAK_UP",
                    "z_score": float(row["close"] - ib_high),
                    "predicted_dir": -1,  # FADE — false breakout → return down
                })
            if not ib_broken_down and row["close"] < ib_low:
                ib_broken_down = True
                sig_data.append({
                    "datetime_utc": row["datetime_utc"],
                    "close": row["close"],
                    "volume": int(row["volume"]),
                    "delta": int(row["delta"]),
                    "signal": "IB_BREAK_DOWN",
                    "z_score": float(ib_low - row["close"]),
                    "predicted_dir": 1,  # FADE — false breakout → return up
                })
            if ib_broken_up and ib_broken_down:
                break

    if not sig_data:
        return pl.DataFrame()

    return pl.DataFrame(sig_data)


# ═══════════════════════════════════════════════════════════════════════════════
# HIT RATE MEASUREMENT
# ═══════════════════════════════════════════════════════════════════════════════

def measure_forward_returns(signals: pl.DataFrame, all_ticks: pl.DataFrame) -> pl.DataFrame:
    """
    For each signal, find the price N seconds later.
    Compute whether the move matches the predicted direction.

    Returns the signals DataFrame enriched with:
        fwd_30s, fwd_60s, fwd_120s  — future price
        hit_30s, hit_60s, hit_120s  — 1 if direction correct, 0 otherwise
    """
    if signals.is_empty():
        return signals

    # Convert timestamps to microseconds for fast lookup
    ticks_ts = all_ticks["datetime_utc"].cast(pl.Int64).to_list()
    ticks_close = all_ticks["close"].to_list()
    n_ticks = len(ticks_ts)

    # Build a fast index
    sig_ts = signals["datetime_utc"].cast(pl.Int64).to_list()
    sig_close = signals["close"].to_list()
    sig_dir = signals["predicted_dir"].to_list()

    results = {h: {"fwd": [], "hit": []} for h in HORIZONS}

    # Search index in all_ticks
    search_idx = 0

    for i in range(len(sig_ts)):
        ts = sig_ts[i]
        price = sig_close[i]
        direction = sig_dir[i]

        # Advance search_idx to the signal tick
        while search_idx < n_ticks - 1 and ticks_ts[search_idx] < ts:
            search_idx += 1

        for h in HORIZONS:
            target_ts = ts + h * 1_000_000  # seconds → microseconds

            # Find the tick closest to target_ts
            j = search_idx
            while j < n_ticks - 1 and ticks_ts[j] < target_ts:
                j += 1

            if j < n_ticks and abs(ticks_ts[j] - target_ts) < 30_000_000:  # 30s tolerance
                fwd_price = ticks_close[j]
                move = fwd_price - price
                hit = 1 if (direction > 0 and move > 0) or (direction < 0 and move < 0) else 0
            else:
                fwd_price = None
                hit = None

            results[h]["fwd"].append(fwd_price)
            results[h]["hit"].append(hit)

    # Add the columns
    for h in HORIZONS:
        signals = signals.with_columns([
            pl.Series(f"fwd_{h}s", results[h]["fwd"]).alias(f"fwd_{h}s"),
            pl.Series(f"hit_{h}s", results[h]["hit"]).alias(f"hit_{h}s"),
        ])

    return signals


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION CALIBRATION
# ═══════════════════════════════════════════════════════════════════════════════

def calibrate_session(ticks: pl.DataFrame, sym: str, thresholds: dict) -> pl.DataFrame:
    """
    Run the 6 detections on a session, measure forward returns.
    Returns a DataFrame of signals with hit rates.
    """
    # Minimum volume filter — eliminates retail noise
    min_vol = thresholds.get("min_volume", MIN_VOLUME.get(sym, 5))
    ticks_filtered = ticks.filter(pl.col("volume") >= min_vol)
    if ticks_filtered.shape[0] < 200:
        return pl.DataFrame()

    tick = TICK_SIZE[sym]
    all_signals = []

    # 1. Large Prints
    lp = detect_large_prints(ticks_filtered, thresholds)
    if not lp.is_empty():
        all_signals.append(lp)

    # 2. Absorption
    ab = detect_absorption(ticks_filtered, thresholds)
    if not ab.is_empty():
        all_signals.append(ab)

    # 3. Iceberg
    ic = detect_iceberg(ticks_filtered, tick, thresholds)
    if not ic.is_empty():
        all_signals.append(ic)

    # 4. Burst
    bu = detect_burst(ticks_filtered, thresholds)
    if not bu.is_empty():
        all_signals.append(bu)

    # 5. Delta Divergence (Phase 3)
    dd = detect_delta_divergence(ticks_filtered, sym, thresholds)
    if not dd.is_empty():
        all_signals.append(dd)

    # 6. Stacked Imbalance (Phase 3)
    si = detect_stacked_imbalance(ticks_filtered, tick, thresholds)
    if not si.is_empty():
        all_signals.append(si)

    # 7. Exhaustion (Phase 3b)
    ex = detect_exhaustion(ticks_filtered, sym, thresholds)
    if not ex.is_empty():
        all_signals.append(ex)

    # 8. Session Context — IB Break + Opening Drive (Phase 3b)
    # Uses UNFILTERED ticks (session events, no minimum volume)
    sc = detect_session_context(ticks, sym, thresholds)
    if not sc.is_empty():
        all_signals.append(sc)

    if not all_signals:
        return pl.DataFrame()

    combined = pl.concat(all_signals, how="diagonal_relaxed").sort("datetime_utc")

    # Filter out signals without direction (delta = 0)
    combined = combined.filter(pl.col("predicted_dir") != 0)

    if combined.is_empty():
        return pl.DataFrame()

    # Measure forward returns on UNFILTERED ticks (real market price)
    combined = measure_forward_returns(combined, ticks)

    return combined


# ═══════════════════════════════════════════════════════════════════════════════
# REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def print_hit_rates(results: pl.DataFrame, symbol: str, thresholds: dict):
    """Print hit rate per signal and per horizon."""
    if results.is_empty():
        print(f"  {YELLOW}No signal detected{RESET}")
        return

    print(f"\n  {BOLD}{CYAN}{'─' * 65}{RESET}")
    print(f"  {BOLD}{symbol} — {NAMES[symbol]} — Hit Rates{RESET}")
    print(f"  {'─' * 65}")

    # Thresholds used
    min_vol = thresholds.get("min_volume", "auto")
    print(f"  {DIM}Thresholds: min_vol={min_vol}  LP_z={thresholds['large_print_z']}  "
          f"BURST_v={thresholds['burst_vol_z']}/s={thresholds['burst_speed_z']}  "
          f"ICE_hits={thresholds['iceberg_min_hits']}/{thresholds['iceberg_window']}  "
          f"ABS_w={thresholds['absorption_window']}{RESET}")

    signals = results["signal"].unique().sort().to_list()
    total_signals = results.shape[0]

    print(f"\n  {'Signal':20s}  {'Count':>7s}  {'30s':>7s}  {'60s':>7s}  {'120s':>7s}  {'Trend':>6s}")
    print(f"  {'─' * 65}")

    for sig in signals:
        sub = results.filter(pl.col("signal") == sig)
        count = sub.shape[0]

        rates = {}
        for h in HORIZONS:
            col = f"hit_{h}s"
            valid = sub.filter(pl.col(col).is_not_null())
            if valid.shape[0] > 0:
                rate = valid[col].mean() * 100
                rates[h] = rate
            else:
                rates[h] = None

        # Color based on 60s hit rate
        r60 = rates.get(60)
        if r60 is not None:
            color = GREEN if r60 >= 55 else YELLOW if r60 >= 50 else RED
        else:
            color = DIM

        # Trend: 120s vs 30s
        r30 = rates.get(30)
        r120 = rates.get(120)
        if r30 is not None and r120 is not None:
            trend = "↑" if r120 > r30 + 1 else "↓" if r120 < r30 - 1 else "→"
        else:
            trend = "?"

        def fmt_rate(r):
            return f"{r:5.1f}%" if r is not None else f"{'n/a':>6s}"

        print(f"  {color}{sig:20s}{RESET}  {count:>7,}  "
              f"{fmt_rate(rates.get(30))}  {fmt_rate(rates.get(60))}  "
              f"{fmt_rate(rates.get(120))}  {trend:>4s}")

    # Total
    print(f"  {'─' * 65}")
    total_rates = {}
    for h in HORIZONS:
        col = f"hit_{h}s"
        valid = results.filter(pl.col(col).is_not_null())
        if valid.shape[0] > 0:
            total_rates[h] = valid[col].mean() * 100
        else:
            total_rates[h] = None

    def fmt_rate(r):
        return f"{r:5.1f}%" if r is not None else f"{'n/a':>6s}"

    print(f"  {BOLD}{'TOTAL':20s}{RESET}  {total_signals:>7,}  "
          f"{fmt_rate(total_rates.get(30))}  {fmt_rate(total_rates.get(60))}  "
          f"{fmt_rate(total_rates.get(120))}")

    # Details per signal: average volume, average z-score
    print(f"\n  {DIM}Details:{RESET}")
    for sig in signals:
        sub = results.filter(pl.col("signal") == sig)
        avg_vol = sub["volume"].mean()
        avg_z = sub["z_score"].mean()
        print(f"    {DIM}{sig:20s}  vol_avg={avg_vol:>10,.0f}  z_avg={avg_z:>6.2f}{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# GRID SEARCH
# ═══════════════════════════════════════════════════════════════════════════════

def grid_search_signal(signal_name: str, detect_fn, ticks_sessions: list,
                       sym: str, base_thresholds: dict, param_name: str,
                       values: list) -> list:
    """
    Test several parameter values for a given signal.
    Return a list of {param_value, count, hit_30, hit_60, hit_120}.
    """
    tick = TICK_SIZE[sym]
    results = []

    for val in values:
        thresholds = base_thresholds.copy()
        thresholds[param_name] = val

        all_hits = {h: [] for h in HORIZONS}
        total_count = 0

        for session in ticks_sessions:
            # Minimum volume filter
            min_vol = thresholds.get("min_volume", MIN_VOLUME.get(sym, 5))
            sess_filtered = session.filter(pl.col("volume") >= min_vol)
            if sess_filtered.shape[0] < 200:
                continue

            if signal_name == "LARGE_PRINT":
                sigs = detect_large_prints(sess_filtered, thresholds)
            elif signal_name.startswith("ABSORPTION"):
                sigs = detect_absorption(sess_filtered, thresholds)
                if not sigs.is_empty():
                    sigs = sigs.filter(pl.col("signal") == signal_name)
            elif signal_name == "ICEBERG":
                sigs = detect_iceberg(sess_filtered, tick, thresholds)
            elif signal_name == "BURST":
                sigs = detect_burst(sess_filtered, thresholds)
            else:
                continue

            if sigs.is_empty():
                continue

            sigs = sigs.filter(pl.col("predicted_dir") != 0)
            if sigs.is_empty():
                continue

            sigs = measure_forward_returns(sigs, session)
            total_count += sigs.shape[0]

            for h in HORIZONS:
                col = f"hit_{h}s"
                valid = sigs.filter(pl.col(col).is_not_null())
                if valid.shape[0] > 0:
                    all_hits[h].extend(valid[col].to_list())

        hit_rates = {}
        for h in HORIZONS:
            if all_hits[h]:
                hit_rates[h] = sum(all_hits[h]) / len(all_hits[h]) * 100
            else:
                hit_rates[h] = None

        results.append({
            "param_value": val,
            "count": total_count,
            "hit_30": hit_rates.get(30),
            "hit_60": hit_rates.get(60),
            "hit_120": hit_rates.get(120),
        })

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# PER-SYMBOL ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_symbol(symbol: str, sample_days: int = None, do_grid: bool = False):
    print(f"\n  {BOLD}{'═' * 60}{RESET}")
    print(f"  {BOLD}{CYAN}  {symbol} — {NAMES[symbol]}{RESET}")
    print(f"  {BOLD}{'═' * 60}{RESET}")

    df = load_training(symbol)
    if df.is_empty():
        return

    # Get RTH dates
    dates = get_rth_dates(df, symbol)
    print(f"  {len(dates)} RTH sessions available")

    # Sample if requested
    if sample_days and sample_days < len(dates):
        import random
        random.seed(42)
        dates = sorted(random.sample(dates, sample_days))
        print(f"  {YELLOW}Sample: {sample_days} sessions{RESET}")

    # ── Evaluation with default thresholds ──
    print(f"\n  {BOLD}Evaluating current thresholds...{RESET}")
    thresholds = DEFAULT_THRESHOLDS.copy()
    all_results = []
    sessions_data = []  # keep for grid search

    for i, date in enumerate(dates):
        ticks = session_ticks(df, symbol, date)
        if ticks.shape[0] < 500:
            continue

        sessions_data.append(ticks)

        result = calibrate_session(ticks, symbol, thresholds)
        if not result.is_empty():
            result = result.with_columns(pl.lit(str(date)).alias("session_date"))
            all_results.append(result)

        # Progress
        if (i + 1) % 50 == 0 or i == len(dates) - 1:
            print(f"    {DIM}... {i + 1}/{len(dates)} sessions{RESET}")

    if all_results:
        combined = pl.concat(all_results, how="diagonal_relaxed")
        print_hit_rates(combined, symbol, thresholds)

        # Save
        out_path = INTEL_DIR / f"{symbol}_Calibration.parquet"
        combined.write_parquet(out_path, compression="zstd")
        size = out_path.stat().st_size / 1e6
        print(f"\n  {GREEN}→ {out_path.name} — {combined.shape[0]:,} signals — {size:.2f} MB{RESET}")
    else:
        print(f"  {YELLOW}No signal across {len(dates)} sessions{RESET}")

    # ── Grid Search ──
    if do_grid and sessions_data:
        print(f"\n  {BOLD}{'─' * 60}{RESET}")
        print(f"  {BOLD}{CYAN}Grid Search — {symbol}{RESET}")
        print(f"  {DIM}Sessions: {len(sessions_data)} — may take a few minutes{RESET}")

        # Limit grid search to 30 sessions max for speed
        grid_sessions = sessions_data[:30] if len(sessions_data) > 30 else sessions_data

        grid_tests = [
            ("LARGE_PRINT",    "large_print_z",     GRID["large_print_z"]),
            ("BURST",          "burst_vol_z",        GRID["burst_vol_z"]),
            ("BURST",          "burst_speed_z",      GRID["burst_speed_z"]),
            ("ICEBERG",        "iceberg_min_hits",   GRID["iceberg_min_hits"]),
            ("ABSORPTION_BULL","absorption_window",  GRID["absorption_window"]),
        ]

        for sig_name, param, values in grid_tests:
            print(f"\n  {BOLD}{sig_name}{RESET} — {param}")
            results = grid_search_signal(sig_name, None, grid_sessions, symbol,
                                         thresholds, param, values)
            # Find the best 60s
            best = None
            for r in results:
                val = r["param_value"]
                cnt = r["count"]
                h60 = r["hit_60"]

                color = DIM
                marker = "  "
                if h60 is not None:
                    color = GREEN if h60 >= 55 else YELLOW if h60 >= 50 else RED
                    if best is None or (h60 > best and cnt >= 20):
                        best = h60
                        marker = " ★"

                h60_str = f"{h60:5.1f}%" if h60 is not None else "  n/a "
                h30_str = f"{r['hit_30']:5.1f}%" if r['hit_30'] is not None else "  n/a "
                h120_str = f"{r['hit_120']:5.1f}%" if r['hit_120'] is not None else "  n/a "
                print(f"    {param}={val:<5}  n={cnt:>6,}  "
                      f"30s={h30_str}  60s={color}{h60_str}{RESET}  "
                      f"120s={h120_str}{marker}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]
    symbol_filter = None
    sample_days = None
    do_grid = False

    for i, a in enumerate(args):
        if a == "--symbol" and i + 1 < len(args):
            symbol_filter = args[i + 1].upper()
        if a == "--sample" and i + 1 < len(args):
            sample_days = int(args[i + 1])
        if a == "--grid":
            do_grid = True

    symbols = [symbol_filter] if symbol_filter else SYMBOLS

    print()
    print(f"  {BOLD}{'═' * 60}{RESET}")
    print(f"  {BOLD}{CYAN}  PULSE — Calibration Phase 2 + 3 + 3b{RESET}")
    print(f"  {BOLD}{'═' * 60}{RESET}")
    print(f"  Source : {TRAINING_DIR}")
    if sample_days:
        print(f"  Sample: {sample_days} days")
    if do_grid:
        print(f"  Mode: Grid Search enabled")
    print()

    t0 = time.time()

    for sym in symbols:
        analyze_symbol(sym, sample_days=sample_days, do_grid=do_grid)

    elapsed = time.time() - t0
    m, s = divmod(int(elapsed), 60)

    print()
    print(f"  {BOLD}{'═' * 60}{RESET}")
    print(f"  {GREEN}Done in {m}m{s:02d}s{RESET}")
    print()


if __name__ == "__main__":
    main()
