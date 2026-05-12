#!/usr/bin/env python3
"""
orderflow_regimes.py — Detect institutional order flow regimes.
Usage: python3 Scripts/orderflow_regimes.py

Analyzes 5m candles to classify each window into a regime:
  1. Absorption    — Large volume, small range, delta opposes price
  2. Compression   — Range contracting, volume falling
  3. Distribution  — Large volume at extremes, momentum declining
  4. Aggression    — Delta + range + volume explode together
  5. Exhaustion    — Volume climax followed by reversal
  6. Iceberg       — Repeated hits at the same price with large volume
  7. Sweep         — Fast crossing of levels + reversal
  8. Initiative    — Trading above/below the VA with conviction
  9. Rotation      — Many levels visited (liquidity search)

Sources: 5m candles (Candles/) + daily volume profile (daily_profile.csv)
Output: Reports/Order_Flow/orderflow_regimes.csv
"""
import polars as pl
import math
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "Data"
CANDLES_DIR = DATA_DIR / "Candles"
PROFILE_DIR = DATA_DIR / "Reports" / "Market_Profile"
REPORTS_DIR = DATA_DIR / "Reports" / "Order_Flow"

TICK_SIZE = {"GC": 0.10, "NQ": 0.25}

# RTH hours
RTH_START = {"GC": 13, "NQ": 13}
RTH_END = {"GC": 22, "NQ": 21}


def load_daily_profile():
    """Load daily_profile.csv for VAH/VAL/POC levels."""
    path = PROFILE_DIR / "daily_profile.csv"
    if not path.exists():
        print(f"  WARN: daily_profile.csv not found — initiative scoring disabled")
        return None
    return pl.read_csv(path, try_parse_dates=True)


def compute_regime_metrics(df, symbol):
    """Compute order flow metrics per 5m candle.

    Uses rolling windows to compare each candle to its recent context
    (lookback of 12 = 1h on 5m).
    """
    lookback = 12   # 12 × 5m = 1 hour
    lookback_s = 36  # 36 × 5m = 3 hours (slow context)

    df = df.with_columns([
        pl.col("datetime_utc").dt.date().alias("date"),
        pl.col("datetime_utc").dt.hour().alias("hour"),
        (pl.col("high") - pl.col("low")).alias("range"),
        (pl.col("close") - pl.col("open")).alias("body"),
        pl.col("delta").cast(pl.Float64).alias("delta_f"),
    ])

    # Filter RTH
    rth_start = RTH_START[symbol]
    rth_end = RTH_END[symbol]
    df = df.filter((pl.col("hour") >= rth_start) & (pl.col("hour") < rth_end))

    if df.shape[0] < lookback_s + 10:
        return None

    # --- Rolling metrics ---
    df = df.with_columns([
        # Volume
        pl.col("volume").rolling_mean(lookback).alias("vol_ma"),
        pl.col("volume").rolling_std(lookback).alias("vol_std"),
        pl.col("volume").rolling_mean(lookback_s).alias("vol_ma_slow"),

        # Range
        pl.col("range").rolling_mean(lookback).alias("range_ma"),
        pl.col("range").rolling_std(lookback).alias("range_std"),
        pl.col("range").rolling_mean(lookback_s).alias("range_ma_slow"),

        # Delta
        pl.col("delta_f").rolling_mean(lookback).alias("delta_ma"),
        pl.col("delta_f").rolling_sum(lookback).alias("delta_cum"),
        pl.col("delta_f").abs().rolling_mean(lookback).alias("abs_delta_ma"),

        # Num trades (tick speed proxy)
        pl.col("num_trades").rolling_mean(lookback).alias("trades_ma"),
    ])

    # Z-scores
    df = df.with_columns([
        ((pl.col("volume") - pl.col("vol_ma")) / (pl.col("vol_std") + 1)).alias("vol_z"),
        ((pl.col("range") - pl.col("range_ma")) / (pl.col("range_std") + 0.001)).alias("range_z"),
    ])

    # Derived ratios
    df = df.with_columns([
        # Delta intensity: what % of volume is directional
        (pl.col("delta_f").abs() / (pl.col("volume").cast(pl.Float64) + 1)).alias("delta_intensity"),

        # Absorption score: large volume but price doesn't move
        (pl.col("volume").cast(pl.Float64) / (pl.col("range") + 0.001)).alias("vol_per_range"),

        # Range contraction: current range vs slow average
        (pl.col("range") / (pl.col("range_ma_slow") + 0.001)).alias("range_contraction"),

        # Volume trend: recent volume vs slow context
        (pl.col("vol_ma") / (pl.col("vol_ma_slow") + 1)).alias("vol_trend"),

        # Body/range ratio (conviction)
        (pl.col("body").abs() / (pl.col("range") + 0.001)).alias("body_ratio"),

        # Price direction (signed)
        pl.when(pl.col("close") > pl.col("open")).then(1)
          .when(pl.col("close") < pl.col("open")).then(-1)
          .otherwise(0).alias("price_dir"),

        # Delta direction
        pl.when(pl.col("delta_f") > 0).then(1)
          .when(pl.col("delta_f") < 0).then(-1)
          .otherwise(0).alias("delta_dir"),
    ])

    # Delta vs price agreement
    df = df.with_columns(
        (pl.col("price_dir") * pl.col("delta_dir")).alias("delta_price_agree"),
    )

    # Rolling price reversal (for exhaustion/sweep)
    df = df.with_columns([
        pl.col("close").shift(-1).alias("next_close"),
        pl.col("close").shift(-2).alias("next2_close"),
    ])
    df = df.with_columns(
        pl.when(
            ((pl.col("close") > pl.col("open")) & (pl.col("next_close") < pl.col("close"))) |
            ((pl.col("close") < pl.col("open")) & (pl.col("next_close") > pl.col("close")))
        ).then(1).otherwise(0).alias("reversal_next"),
    )

    # Iceberg proxy: how many times the high or low is hit
    # (same high/low as the previous candle = possible hidden order)
    tick = TICK_SIZE[symbol]
    df = df.with_columns([
        (pl.col("high") - pl.col("high").shift(1)).abs().alias("high_diff"),
        (pl.col("low") - pl.col("low").shift(1)).abs().alias("low_diff"),
    ])
    df = df.with_columns(
        pl.when(
            (pl.col("high_diff") <= tick * 3) | (pl.col("low_diff") <= tick * 3)
        ).then(1).otherwise(0).alias("same_level_hit"),
    )
    df = df.with_columns(
        pl.col("same_level_hit").rolling_sum(6).alias("level_hits_30m"),
    )

    # Rotation: how many price levels visited (range / tick)
    df = df.with_columns(
        (pl.col("range") / tick).round(0).cast(pl.Int64).alias("levels_visited"),
    )
    df = df.with_columns(
        pl.col("levels_visited").rolling_sum(6).alias("rotation_30m"),
    )

    return df


def classify_regimes(df, symbol, daily_profile=None):
    """Classify each 5m candle into an order flow regime.

    Rotation and Iceberg thresholds are normalized by the symbol's tick size
    to avoid NQ (tick=0.25, range~250) being classified Rotation 93% of the
    time while GC (tick=0.10, range~27) is under-detected.
    """
    tick = TICK_SIZE[symbol]

    # Symbol-adaptive thresholds
    # Rotation: use a z-score of rotation instead of an absolute threshold
    # Iceberg: same-price ±N ticks depends on tick size

    # Join VAH/VAL/POC if available
    if daily_profile is not None:
        dp = daily_profile.filter(pl.col("symbol") == symbol).select([
            pl.col("date").cast(pl.Date),
            "poc", "vah", "val",
        ])
        df = df.join(dp, on="date", how="left")
        has_profile = True
    else:
        has_profile = False

    # Precompute rotation z-score (normalize by context)
    df = df.with_columns([
        pl.col("rotation_30m").rolling_mean(72).alias("rotation_ma"),   # 6h context
        pl.col("rotation_30m").rolling_std(72).alias("rotation_std"),
    ])
    df = df.with_columns(
        ((pl.col("rotation_30m") - pl.col("rotation_ma")) / (pl.col("rotation_std") + 1)).alias("rotation_z"),
    )

    regimes = []

    for row in df.iter_rows(named=True):
        flags = []
        scores = {}

        vol_z = row.get("vol_z")
        range_z = row.get("range_z")
        delta_intensity = row.get("delta_intensity")
        vol_per_range = row.get("vol_per_range")
        range_contraction = row.get("range_contraction")
        vol_trend = row.get("vol_trend")
        body_ratio = row.get("body_ratio")
        delta_price_agree = row.get("delta_price_agree")
        reversal_next = row.get("reversal_next")
        level_hits = row.get("level_hits_30m")
        rotation_z = row.get("rotation_z")
        delta_cum = row.get("delta_cum")

        if any(v is None for v in [vol_z, range_z]):
            regimes.append({"regime": "", "score": 0, "flags": ""})
            continue

        # --- ABSORPTION ---
        # Large volume + small range + delta opposes price
        # Loosened: vol_z > 0.5 (instead of 1.0), delta_intensity > 0.2
        if vol_z > 0.5 and range_z < 0.5 and delta_intensity is not None and delta_intensity > 0.2:
            if delta_price_agree is not None and delta_price_agree <= 0:
                score = min(1.0, (vol_z / 2.5) * max(0, 1 - range_z))
                scores["Absorption"] = score
                flags.append("ABSORPTION")

        # --- COMPRESSION ---
        # Range contracting + volume falling
        if range_contraction is not None and range_contraction < 0.6 and vol_trend is not None and vol_trend < 0.8:
            score = min(1.0, (1 - range_contraction) * (1 - vol_trend))
            scores["Compression"] = score
            flags.append("COMPRESSION")

        # --- DISTRIBUTION ---
        # Large volume + small body relative to range (indecision at top)
        if vol_z > 0.5 and body_ratio is not None and body_ratio < 0.3 and range_z > 0:
            score = min(1.0, vol_z / 3 * (1 - body_ratio))
            scores["Distribution"] = score
            flags.append("DISTRIBUTION")

        # --- AGGRESSION ---
        # Delta + range + volume all high, same direction
        if vol_z > 1.0 and range_z > 1.0 and delta_price_agree is not None and delta_price_agree > 0:
            if body_ratio is not None and body_ratio > 0.5:
                score = min(1.0, (vol_z + range_z) / 5 * body_ratio)
                scores["Aggression"] = score
                flags.append("AGGRESSION")

        # --- EXHAUSTION ---
        # Volume climax + immediate reversal
        if vol_z > 1.5 and reversal_next is not None and reversal_next > 0:
            score = min(1.0, vol_z / 3)
            scores["Exhaustion"] = score
            flags.append("EXHAUSTION")

        # --- ICEBERG ---
        # Repeated hits at the same level with volume
        if level_hits is not None and level_hits >= 4 and vol_z > -0.5:
            score = min(1.0, level_hits / 6 * max(0.3, min(1, vol_z + 0.5)))
            scores["Iceberg"] = score
            flags.append("ICEBERG")

        # --- SWEEP ---
        # Large range + quick reversal (long wick, small body)
        if range_z > 1.5 and reversal_next is not None and reversal_next > 0:
            if body_ratio is not None and body_ratio < 0.35:
                score = min(1.0, range_z / 3)
                scores["Sweep"] = score
                flags.append("SWEEP")

        # --- INITIATIVE ---
        if has_profile and row.get("vah") is not None and row.get("val") is not None:
            close = row["close"]
            vah = row["vah"]
            val_ = row["val"]
            if close > vah and delta_cum is not None and delta_cum > 0:
                score = 0.6
                scores["Initiative_Buy"] = score
                flags.append("INITIATIVE_BUY")
            elif close < val_ and delta_cum is not None and delta_cum < 0:
                score = 0.6
                scores["Initiative_Sell"] = score
                flags.append("INITIATIVE_SELL")

        # --- ROTATION ---
        # Use z-score instead of absolute threshold (normalizes GC vs NQ)
        if rotation_z is not None and rotation_z > 1.5:
            score = min(1.0, rotation_z / 3)
            scores["Rotation"] = score
            flags.append("ROTATION")

        # Dominant regime = the one with the highest score
        if scores:
            dominant = max(scores, key=scores.get)
            regimes.append({
                "regime": dominant,
                "score": round(scores[dominant], 2),
                "flags": ", ".join(flags),
            })
        else:
            regimes.append({"regime": "Neutral", "score": 0, "flags": ""})

    return pl.DataFrame(regimes)


def analyze_symbol(symbol, daily_profile):
    """Full analysis of one symbol."""
    path = CANDLES_DIR / f"{symbol}_5m.parquet"
    if not path.exists():
        print(f"  {symbol}: 5m candles not found")
        return None

    print(f"\n  {symbol}: loading 5m candles...", end=" ", flush=True)
    df = pl.read_parquet(path)
    print(f"{df.shape[0]:,} candles")

    print(f"  Computing metrics...", end=" ", flush=True)
    df = compute_regime_metrics(df, symbol)
    if df is None:
        print("not enough data")
        return None
    print("OK")

    print(f"  Classifying regimes...", end=" ", flush=True)
    regime_df = classify_regimes(df, symbol, daily_profile)
    print("OK")

    # Combine
    export_cols = [
        "datetime_utc", "date", "hour", "open", "high", "low", "close",
        "volume", "bid_vol", "ask_vol", "delta", "num_trades",
        "range", "body", "vol_z", "range_z",
        "delta_intensity", "vol_per_range", "range_contraction",
        "body_ratio", "delta_price_agree", "level_hits_30m", "rotation_30m",
    ]
    # Only select columns that exist
    available = df.columns
    export_cols = [c for c in export_cols if c in available]

    result = pl.concat([
        df.select(export_cols),
        regime_df,
    ], how="horizontal")

    result = result.with_columns(pl.lit(symbol).alias("symbol"))

    # --- Stats ---
    total = result.shape[0]
    regime_counts = result.filter(pl.col("regime") != "").group_by("regime").agg(
        pl.len().alias("count"),
        pl.col("score").mean().alias("avg_score"),
    ).sort("count", descending=True)

    print(f"\n  ── Regimes detected: {symbol} ({total:,} RTH candles) ──")
    for row in regime_counts.iter_rows(named=True):
        pct = row["count"] / total * 100
        bar = "█" * int(pct / 2)
        print(f"    {row['regime']:18s}  {row['count']:5d} ({pct:5.1f}%)  "
              f"avg score {row['avg_score']:.2f}  {bar}")

    # Regimes per hour
    print(f"\n  ── Regimes per hour (top 3 per hour) ──")
    hours = sorted(result.filter(pl.col("regime") != "Neutral")["hour"].unique().to_list())
    for h in hours:
        h_data = result.filter((pl.col("hour") == h) & (pl.col("regime") != "Neutral"))
        if h_data.shape[0] < 5:
            continue
        h_counts = h_data.group_by("regime").agg(pl.len().alias("n")).sort("n", descending=True).head(3)
        top_str = "  ".join(f"{r['regime']}:{r['n']}" for r in h_counts.iter_rows(named=True))
        print(f"    {h:02d}h UTC  {top_str}")

    return result


def main():
    print("=" * 65)
    print("  EAGLE — Order Flow Regime Detection")
    print("=" * 65)

    daily_profile = load_daily_profile()

    all_results = []
    for symbol in ["GC", "NQ"]:
        result = analyze_symbol(symbol, daily_profile)
        if result is not None:
            all_results.append(result)

    if all_results:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        combined = pl.concat(all_results).sort(["symbol", "datetime_utc"])

        csv_path = REPORTS_DIR / "orderflow_regimes.csv"
        combined.write_csv(csv_path)
        print(f"\n  CSV saved: {csv_path}")
        print(f"    {combined.shape[0]:,} rows × {combined.shape[1]} columns")

        # Summary counts
        print(f"\n  ── Global summary ──")
        for symbol in ["GC", "NQ"]:
            sym = combined.filter(pl.col("symbol") == symbol)
            active = sym.filter(pl.col("regime") != "Neutral")
            print(f"    {symbol}: {active.shape[0]:,} candles with active regime "
                  f"/ {sym.shape[0]:,} total ({active.shape[0]/sym.shape[0]*100:.1f}%)")

    print(f"\n  Done.")


if __name__ == "__main__":
    main()
