#!/usr/bin/env python3
"""
build_features.py — Build ML features from the pre-aggregated candles.
Usage: python3 Scripts/build_features.py

Reads from Data/Candles/, writes to Data/Features/
Features computed on 5m candles (noise/signal trade-off).
Target: range of the next candle (volatility prediction).

See note_ml.md for feature documentation.
"""
import polars as pl
from pathlib import Path
import math
import gc

DATA_DIR = Path(__file__).parent.parent / "Data"    # Eagle/Data/
CANDLES_DIR = DATA_DIR / "Candles"
FEATURES_DIR = DATA_DIR / "Features"

SYMBOLS = ["GC", "NQ"]

# Timeframes to process (more can be added later)
TIMEFRAMES = ["5m"]


def build_features(symbol, timeframe):
    """
    Compute ML features for a symbol and a timeframe.
    Returns a DataFrame with features + target, ML-ready.
    """
    path = CANDLES_DIR / f"{symbol}_{timeframe}.parquet"
    if not path.exists():
        print(f"  {symbol} {timeframe}: file not found")
        return None

    df = pl.read_parquet(path)
    n = df.shape[0]
    print(f"\n  {symbol} {timeframe}: {n:,} candles")

    # ═══════════════════════════════════════════════════════════════
    # BASE COLUMNS
    # ═══════════════════════════════════════════════════════════════

    df = df.with_columns([
        # Return in points
        (pl.col("close") - pl.col("open")).alias("return"),
        # Range
        (pl.col("high") - pl.col("low")).alias("range"),
        # Delta (already present, but make sure)
        (pl.col("ask_vol").cast(pl.Int64) - pl.col("bid_vol").cast(pl.Int64)).alias("delta"),
        # Hour and day
        pl.col("datetime_utc").dt.hour().alias("hour"),
        pl.col("datetime_utc").dt.weekday().alias("weekday"),
    ])

    # ═══════════════════════════════════════════════════════════════
    # TIER 1 — Strong signal
    # ═══════════════════════════════════════════════════════════════

    print("    Tier 1: order flow, vol regime, hour, imbalance...")

    df = df.with_columns([
        # --- Order flow: cumulative delta over rolling windows ---
        pl.col("delta").rolling_sum(window_size=3).alias("delta_cum_3"),
        pl.col("delta").rolling_sum(window_size=6).alias("delta_cum_6"),
        pl.col("delta").rolling_sum(window_size=12).alias("delta_cum_12"),

        # --- Volatility: rolling std of return ---
        pl.col("return").rolling_std(window_size=20).alias("vol_20"),
        pl.col("return").rolling_std(window_size=100).alias("vol_100"),

        # --- Rolling range (for target and features) ---
        pl.col("range").rolling_mean(window_size=20).alias("range_ma20"),
        pl.col("range").rolling_mean(window_size=100).alias("range_ma100"),

        # --- Rolling bid/ask imbalance ---
        pl.col("ask_vol").rolling_sum(window_size=6).alias("ask_sum_6"),
        pl.col("bid_vol").rolling_sum(window_size=6).alias("bid_sum_6"),
        pl.col("ask_vol").rolling_sum(window_size=20).alias("ask_sum_20"),
        pl.col("bid_vol").rolling_sum(window_size=20).alias("bid_sum_20"),
    ])

    # Derived ratios (require the columns above)
    df = df.with_columns([
        # Vol regime: short/long term
        (pl.col("vol_20") / pl.col("vol_100")).alias("vol_regime"),

        # Range ratio: current / MA (epsilon to avoid div/0)
        (pl.col("range") / (pl.col("range_ma20") + 0.001)).alias("range_ratio"),

        # Imbalance 6 periods
        ((pl.col("ask_sum_6") - pl.col("bid_sum_6")) /
         (pl.col("ask_sum_6") + pl.col("bid_sum_6") + 1)).alias("imbalance_6"),

        # Imbalance 20 periods
        ((pl.col("ask_sum_20") - pl.col("bid_sum_20")) /
         (pl.col("ask_sum_20") + pl.col("bid_sum_20") + 1)).alias("imbalance_20"),

        # Cyclic encoding of the hour (sin/cos)
        (pl.col("hour").cast(pl.Float64) * (2.0 * math.pi / 24.0)).sin().alias("hour_sin"),
        (pl.col("hour").cast(pl.Float64) * (2.0 * math.pi / 24.0)).cos().alias("hour_cos"),

        # Cyclic encoding of the day (sin/cos)
        (pl.col("weekday").cast(pl.Float64) * (2.0 * math.pi / 5.0)).sin().alias("day_sin"),
        (pl.col("weekday").cast(pl.Float64) * (2.0 * math.pi / 5.0)).cos().alias("day_cos"),
    ])

    # ═══════════════════════════════════════════════════════════════
    # TIER 2 — Probable signal
    # ═══════════════════════════════════════════════════════════════

    print("    Tier 2: trade intensity, volume ratio, lagged returns...")

    df = df.with_columns([
        # --- Trade intensity (normalized num_trades) ---
        pl.col("num_trades").rolling_mean(window_size=20).alias("trades_ma20"),
    ])

    df = df.with_columns([
        (pl.col("num_trades") / (pl.col("trades_ma20") + 1)).alias("trade_intensity"),

        # --- Volume ratio (vs MA) ---
        pl.col("volume").rolling_mean(window_size=20).alias("vol_ma20"),
    ])

    df = df.with_columns([
        (pl.col("volume") / (pl.col("vol_ma20") + 1)).alias("volume_ratio"),

        # --- Lagged returns (capture momentum/mean-reversion) ---
        pl.col("return").shift(1).alias("ret_lag1"),
        pl.col("return").shift(2).alias("ret_lag2"),
        pl.col("return").shift(3).alias("ret_lag3"),
        pl.col("return").shift(5).alias("ret_lag5"),

        # --- Lagged range ---
        pl.col("range").shift(1).alias("range_lag1"),
        pl.col("range").shift(2).alias("range_lag2"),
    ])

    # ═══════════════════════════════════════════════════════════════
    # TARGET — What we are trying to predict
    # ═══════════════════════════════════════════════════════════════

    print("    Target: range of the next candle...")

    df = df.with_columns([
        # Main target: range of the next candle (future volatility)
        pl.col("range").shift(-1).alias("target_range_next"),

        # Alternative target: direction (1 if return positive, 0 otherwise)
        (pl.col("return").shift(-1) > 0).cast(pl.Int8).alias("target_direction_next"),

        # Alternative target: raw return of the next candle
        pl.col("return").shift(-1).alias("target_return_next"),
    ])

    # ═══════════════════════════════════════════════════════════════
    # FINAL SELECTION
    # ═══════════════════════════════════════════════════════════════

    # Feature columns (what the model sees)
    feature_cols = [
        # Tier 1
        "delta_cum_3", "delta_cum_6", "delta_cum_12",
        "vol_20", "vol_100", "vol_regime",
        "range_ratio",
        "imbalance_6", "imbalance_20",
        "hour_sin", "hour_cos",
        "day_sin", "day_cos",
        # Tier 2
        "trade_intensity", "volume_ratio",
        "ret_lag1", "ret_lag2", "ret_lag3", "ret_lag5",
        "range_lag1", "range_lag2",
    ]

    # Target columns
    target_cols = [
        "target_range_next",
        "target_direction_next",
        "target_return_next",
    ]

    # Context columns (not features, but useful for analysis)
    context_cols = [
        "datetime_utc", "open", "high", "low", "close",
        "volume", "delta", "range", "return",
    ]

    # Select and clean
    all_cols = context_cols + feature_cols + target_cols
    result = df.select(all_cols)

    # Drop rows with nulls in features (start of series + last row)
    n_before = result.shape[0]
    result = result.drop_nulls(subset=feature_cols + ["target_range_next"])
    n_after = result.shape[0]
    dropped = n_before - n_after

    print(f"    Rows dropped (warmup + end): {dropped}")
    print(f"    Final dataset: {n_after:,} rows × {len(feature_cols)} features")

    # Quick feature stats
    print(f"\n    ── Feature stats ──")
    for col in feature_cols:
        s = result[col].drop_nulls()
        print(f"    {col:20s}  avg {s.mean():>10.4f}  std {s.std():>10.4f}  "
              f"min {s.min():>10.4f}  max {s.max():>10.4f}")

    # Target stats
    print(f"\n    ── Target stats ──")
    t = result["target_range_next"].drop_nulls()
    print(f"    target_range_next   avg {t.mean():.4f}  std {t.std():.4f}  "
          f"med {t.median():.4f}  P95 {t.quantile(0.95):.4f}")

    d = result["target_direction_next"]
    print(f"    target_direction    % up {d.mean()*100:.1f}%")

    return result


def main():
    print("=" * 65)
    print("  EAGLE — Build Features (ML-ready datasets)")
    print("=" * 65)

    FEATURES_DIR.mkdir(parents=True, exist_ok=True)

    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            result = build_features(symbol, tf)

            if result is not None:
                out_path = FEATURES_DIR / f"{symbol}_{tf}_features.parquet"
                result.write_parquet(out_path, compression="zstd")
                size_mb = out_path.stat().st_size / 1e6
                print(f"\n    → {out_path.name} ({size_mb:.1f} MB)")
                print(f"  ✓ {symbol} {tf} done")

                del result
                gc.collect()

    # Summary
    print(f"\n{'═'*65}")
    print(f"  Summary — {FEATURES_DIR}")
    print(f"{'═'*65}")
    for f in sorted(FEATURES_DIR.glob("*.parquet")):
        lf = pl.scan_parquet(f)
        rows = lf.select(pl.len()).collect().item()
        cols = len(lf.collect_schema().names())
        size_mb = f.stat().st_size / 1e6
        print(f"  {f.name:30s}  {rows:>10,} rows  {cols} cols  {size_mb:>5.1f} MB")

    print(f"\n  Done. Datasets ready for ML.")


if __name__ == "__main__":
    main()
