#!/usr/bin/env python3
"""
market_profile.py — Classify days by Market Profile type.
Usage: python3 Scripts/market_profile.py

Computes the Initial Balance (IB) for each RTH day,
measures extensions, and classifies the day type.

IB = range of the first RTH hour:
  GC : 13:00-14:00 UTC (COMEX open 13:20)
  NQ : 14:00-15:00 UTC (Equity open 14:30)

Day types:
  - Trend       : narrow IB, strong unidirectional extension (range > 2.5× IB)
  - Normal      : wide IB, little extension (range < 1.3× IB)
  - Normal Var  : moderate IB, one-sided extension (1.3-2.5× IB, asymmetric)
  - Neutral     : extensions on both sides (ratio < 1.5)
  - Double Dist : migration to a 2nd zone (detected via intraday gap)
  - Non-Trend   : very narrow total range (< 0.6× median)
"""
import polars as pl
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "Data"    # Eagle/Data/
CANDLES_DIR = DATA_DIR / "Candles"
REPORTS_DIR = DATA_DIR / "Reports" / "Market_Profile"

# IB start hour (UTC)
IB_HOUR = {
    "GC": 13,   # COMEX open 13:20 UTC
    "NQ": 14,   # Equity open 14:30 UTC
}

# RTH hours used to measure the day's range
RTH_START = {
    "GC": 13,   # 13:00 UTC = 8:00 AM ET
    "NQ": 13,   # 13:00 UTC = 8:00 AM ET (pre-market + open)
}
RTH_END = {
    "GC": 22,   # 22:00 UTC = 5:00 PM ET
    "NQ": 21,   # 21:00 UTC = 4:00 PM ET
}

TICK_SIZE = {
    "GC": 0.10,   # Gold: tick = 0.10
    "NQ": 0.25,   # Nasdaq: tick = 0.25
}

DAYS_EN = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def build_tpo_profile(rth_candles_df, symbol):
    """Build the daily TPO profile → POC, VAH, VAL.

    True TPO like Sierra Chart:
      - Slice the RTH session into 30-minute periods
      - Each period gets a letter (A, B, C, ...)
      - For each period, mark all prices (per tick) touched
      - POC  = price with the most letters (TPO count)
      - VAH/VAL = bounds of the Value Area (70% of TPO counts around the POC)
    """
    tick = TICK_SIZE[symbol]
    rth_start = RTH_START[symbol]
    rth_end = RTH_END[symbol]
    results = []

    # TPO letters: A-Z then a-z (52 periods max = 26h, more than enough)
    LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    for date_val, group in rth_candles_df.group_by("date"):
        date_key = date_val[0] if isinstance(date_val, tuple) else date_val
        tpo_at_price = {}  # price → set of letters

        # Build 30-min periods and assign letters
        # Period 0 = RTH_START:00-RTH_START:30, Period 1 = RTH_START:30-RTH_END:00, etc.
        for row in group.iter_rows(named=True):
            dt = row["datetime_utc"]
            hour = dt.hour
            minute = dt.minute

            # Compute the 30-min period index since the RTH start
            minutes_from_start = (hour - rth_start) * 60 + minute
            if minutes_from_start < 0:
                continue
            period_idx = minutes_from_start // 30

            if period_idx >= len(LETTERS):
                continue
            letter = LETTERS[period_idx]

            low = row["low"]
            high = row["high"]
            if high <= low:
                continue

            # Mark each price touched by this letter
            n_levels = max(1, round((high - low) / tick))
            for i in range(n_levels + 1):
                price = round(low + i * tick, 4)
                if price not in tpo_at_price:
                    tpo_at_price[price] = set()
                tpo_at_price[price].add(letter)

        if not tpo_at_price:
            results.append({"date": date_key, "poc": None, "vah": None, "val": None})
            continue

        # TPO count = number of distinct letters at each price
        tpo_count = {p: len(letters) for p, letters in tpo_at_price.items()}

        # POC = price with the most TPO counts
        poc = max(tpo_count, key=tpo_count.get)
        total_tpo = sum(tpo_count.values())
        target_tpo = total_tpo * 0.70

        # Value Area: extend from the POC upward and downward
        sorted_prices = sorted(tpo_count.keys())
        poc_idx = sorted_prices.index(poc)
        va_tpo = tpo_count[poc]
        lo_idx = poc_idx
        hi_idx = poc_idx

        while va_tpo < target_tpo and (lo_idx > 0 or hi_idx < len(sorted_prices) - 1):
            tpo_up = tpo_count[sorted_prices[hi_idx + 1]] if hi_idx < len(sorted_prices) - 1 else 0
            tpo_down = tpo_count[sorted_prices[lo_idx - 1]] if lo_idx > 0 else 0

            if tpo_up >= tpo_down:
                hi_idx += 1
                va_tpo += tpo_count[sorted_prices[hi_idx]]
            else:
                lo_idx -= 1
                va_tpo += tpo_count[sorted_prices[lo_idx]]

        vah = sorted_prices[hi_idx]
        val_ = sorted_prices[lo_idx]

        results.append({"date": date_key, "poc": poc, "vah": vah, "val": val_})

    return pl.DataFrame(results).with_columns(pl.col("date").cast(pl.Date))


def classify_days(symbol):
    """Classify each trading day by Market Profile type."""

    # Load 5m candles for good granularity
    path = CANDLES_DIR / f"{symbol}_5m.parquet"
    if not path.exists():
        print(f"  {symbol}: file not found")
        return None

    df = pl.read_parquet(path)

    ib_hour = IB_HOUR[symbol]
    rth_start = RTH_START[symbol]
    rth_end = RTH_END[symbol]

    # Add time columns
    df = df.with_columns([
        pl.col("datetime_utc").dt.date().alias("date"),
        pl.col("datetime_utc").dt.hour().alias("hour"),
        pl.col("datetime_utc").dt.weekday().alias("weekday"),
    ])

    # --- IB: first RTH hour ---
    ib_candles = df.filter(pl.col("hour") == ib_hour)
    ib_daily = ib_candles.group_by("date").agg([
        pl.col("high").max().alias("ib_high"),
        pl.col("low").min().alias("ib_low"),
        pl.col("volume").sum().alias("ib_volume"),
        pl.col("weekday").first().alias("weekday"),
    ]).with_columns(
        (pl.col("ib_high") - pl.col("ib_low")).alias("ib_range"),
    )

    # --- RTH: total daily range ---
    rth_candles = df.filter(
        (pl.col("hour") >= rth_start) & (pl.col("hour") < rth_end)
    )
    rth_daily = rth_candles.group_by("date").agg([
        pl.col("high").max().alias("day_high"),
        pl.col("low").min().alias("day_low"),
        pl.col("volume").sum().alias("day_volume"),
        pl.col("open").first().alias("day_open"),
        pl.col("close").last().alias("day_close"),
    ]).with_columns(
        (pl.col("day_high") - pl.col("day_low")).alias("day_range"),
    )

    # --- TPO Profile: POC, VAH, VAL ---
    print(f"  Computing TPO profile (30min letters)...", end=" ", flush=True)
    vp = build_tpo_profile(rth_candles, symbol)
    print(f"OK ({vp.shape[0]} days)")

    # Join IB + RTH + Volume Profile
    days = ib_daily.join(rth_daily, on="date", how="inner")
    days = days.join(vp, on="date", how="left").sort("date")

    # Filter days with a valid IB (> 0)
    days = days.filter(pl.col("ib_range") > 0)

    n_days = days.shape[0]
    median_ib = days["ib_range"].median()
    median_range = days["day_range"].median()

    print(f"\n  {symbol}: {n_days} RTH days analyzed")
    print(f"  IB hour: {ib_hour}:00 UTC")
    print(f"  Median IB: {median_ib:.2f} points")
    print(f"  Median RTH range: {median_range:.2f} points")

    # --- Classification ---
    # Compute extension metrics
    days = days.with_columns([
        # Extension above IB
        (pl.col("day_high") - pl.col("ib_high")).clip(lower_bound=0).alias("ext_above"),
        # Extension below IB
        (pl.col("ib_low") - pl.col("day_low")).clip(lower_bound=0).alias("ext_below"),
        # range/IB ratio
        (pl.col("day_range") / pl.col("ib_range")).alias("range_ib_ratio"),
    ])

    # Classify each day
    day_types = []
    for row in days.iter_rows(named=True):
        ratio = row["range_ib_ratio"]
        ext_above = row["ext_above"]
        ext_below = row["ext_below"]
        total_ext = ext_above + ext_below
        ib_range = row["ib_range"]
        day_range = row["day_range"]

        # Extension asymmetry
        if total_ext > 0:
            ext_ratio = max(ext_above, ext_below) / (total_ext + 0.001)
        else:
            ext_ratio = 0.5

        # Classification
        if day_range < median_range * 0.5:
            day_type = "Non-Trend"
        elif ratio >= 2.5 and ext_ratio > 0.7:
            day_type = "Trend"
        elif ratio < 1.3:
            day_type = "Normal"
        elif ext_ratio > 0.7:
            day_type = "Normal Var"
        elif ext_ratio < 0.6:
            day_type = "Neutral"
        else:
            day_type = "Normal Var"

        day_types.append(day_type)

    days = days.with_columns(
        pl.Series("day_type", day_types)
    )

    # --- Results ---
    print(f"\n  ── Day type distribution ──")
    type_counts = days.group_by("day_type").agg(
        pl.len().alias("count"),
        pl.col("day_range").mean().alias("avg_range"),
        pl.col("ib_range").mean().alias("avg_ib"),
        pl.col("range_ib_ratio").mean().alias("avg_ratio"),
        pl.col("day_volume").mean().alias("avg_volume"),
    ).sort("count", descending=True)

    for row in type_counts.iter_rows(named=True):
        pct = row["count"] / n_days * 100
        bar = "█" * int(pct / 2)
        print(f"    {row['day_type']:12s}  {row['count']:4d} ({pct:5.1f}%)  "
              f"IB {row['avg_ib']:>7.1f}  range {row['avg_range']:>7.1f}  "
              f"ratio {row['avg_ratio']:>4.1f}x  {bar}")

    # --- By weekday ---
    print(f"\n  ── Types per weekday ──")

    # Crosstab
    weekdays_in_data = sorted(days["weekday"].unique().to_list())

    # Header
    types_list = type_counts["day_type"].to_list()
    header = f"    {'':12s}"
    for t in types_list:
        header += f"  {t:>10s}"
    header += f"  {'Total':>6s}"
    print(header)

    for wd in weekdays_in_data:
        wd_name = DAYS_EN.get(wd, f"?{wd}")
        wd_days = days.filter(pl.col("weekday") == wd)
        wd_total = wd_days.shape[0]
        if wd_total < 5:
            continue

        line = f"    {wd_name:12s}"
        for t in types_list:
            count = wd_days.filter(pl.col("day_type") == t).shape[0]
            pct = count / wd_total * 100
            line += f"  {pct:>9.1f}%"
        line += f"  {wd_total:>5d}d"
        print(line)

    # --- IB stats per weekday ---
    print(f"\n  ── Average IB per weekday ──")
    for wd in weekdays_in_data:
        wd_name = DAYS_EN.get(wd, f"?{wd}")
        wd_days = days.filter(pl.col("weekday") == wd)
        if wd_days.shape[0] < 5:
            continue
        ib_med = wd_days["ib_range"].median()
        ib_mean = wd_days["ib_range"].mean()
        range_med = wd_days["day_range"].median()
        n = wd_days.shape[0]
        bar = "█" * int(ib_med / median_ib * 15)
        print(f"    {wd_name:4s}  IB avg {ib_mean:>7.2f}  med {ib_med:>7.2f}  "
              f"range med {range_med:>7.2f}  (n={n})  {bar}")

    # --- Trend days: detail ---
    print(f"\n  ── Trend Days (detail) ──")
    trend_days = days.filter(pl.col("day_type") == "Trend").sort("day_range", descending=True)
    n_trend = trend_days.shape[0]
    if n_trend > 0:
        print(f"    Total: {n_trend} days ({n_trend/n_days*100:.1f}%)")
        print(f"    Average range: {trend_days['day_range'].mean():.2f}")
        print(f"    Average IB: {trend_days['ib_range'].mean():.2f}")

        # Trend day direction
        trend_up = trend_days.filter(pl.col("ext_above") > pl.col("ext_below")).shape[0]
        trend_down = n_trend - trend_up
        print(f"    Direction: {trend_up} up ({trend_up/n_trend*100:.0f}%) / "
              f"{trend_down} down ({trend_down/n_trend*100:.0f}%)")

        # Top 5 largest trend days
        print(f"    Top 5:")
        for row in trend_days.head(5).iter_rows(named=True):
            wd_name = DAYS_EN.get(row["weekday"], "?")
            direction = "↑" if row["ext_above"] > row["ext_below"] else "↓"
            print(f"      {row['date']} ({wd_name})  range {row['day_range']:.1f}  "
                  f"IB {row['ib_range']:.1f}  ratio {row['range_ib_ratio']:.1f}x  {direction}")
    else:
        print(f"    No trend day detected")

    return days


def save_reports(all_days):
    """Save per-day CSV + text summary."""
    REPORTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # --- CSV: one row per day, both symbols ---
    frames = []
    for symbol, days in all_days.items():
        if days is None:
            continue
        export = days.select([
            pl.lit(symbol).alias("symbol"),
            "date", "weekday",
            "ib_high", "ib_low", "ib_range", "ib_volume",
            "day_high", "day_low", "day_range", "day_volume",
            "day_open", "day_close",
            "poc", "vah", "val",
            "ext_above", "ext_below", "range_ib_ratio",
            "day_type",
        ]).with_columns(
            (pl.col("day_close") - pl.col("day_open")).alias("day_return"),
            (pl.col("vah") - pl.col("val")).alias("va_width"),
            pl.when(pl.col("ext_above") > pl.col("ext_below"))
              .then(pl.lit("up"))
              .otherwise(pl.lit("down"))
              .alias("direction"),
        )
        frames.append(export)

    if frames:
        combined = pl.concat(frames).sort(["symbol", "date"])
        csv_path = REPORTS_DIR / "daily_profile.csv"
        combined.write_csv(csv_path)
        print(f"\n  CSV saved: {csv_path}")
        print(f"    {combined.shape[0]} rows × {combined.shape[1]} columns")

    # --- Text summary ---
    txt_path = REPORTS_DIR / f"market_profile_{timestamp}.txt"
    lines = []
    lines.append("=" * 65)
    lines.append("  EAGLE — Market Profile Summary")
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 65)

    for symbol, days in all_days.items():
        if days is None:
            continue
        n = days.shape[0]
        med_ib = days["ib_range"].median()
        med_range = days["day_range"].median()
        lines.append(f"\n  {symbol} — {n} days")
        lines.append(f"  Median IB: {med_ib:.2f}  Median range: {med_range:.2f}")
        lines.append("")

        # Distribution
        type_counts = days.group_by("day_type").agg(
            pl.len().alias("count"),
            pl.col("day_range").mean().alias("avg_range"),
            pl.col("ib_range").mean().alias("avg_ib"),
        ).sort("count", descending=True)

        for row in type_counts.iter_rows(named=True):
            pct = row["count"] / n * 100
            lines.append(f"    {row['day_type']:12s}  {row['count']:4d} ({pct:5.1f}%)  "
                         f"IB {row['avg_ib']:>7.1f}  range {row['avg_range']:>7.1f}")

        # Trend direction
        trend = days.filter(pl.col("day_type") == "Trend")
        if trend.shape[0] > 0:
            up = trend.filter(pl.col("ext_above") > pl.col("ext_below")).shape[0]
            down = trend.shape[0] - up
            lines.append(f"\n    Trend: {up} up / {down} down")

    lines.append("\n" + "=" * 65)
    txt_path.write_text("\n".join(lines))
    print(f"  Summary saved: {txt_path}")


def build_naked_pocs(all_days):
    """Identify Naked POCs — historical POCs never revisited.

    A POC is 'naked' as long as price has not crossed that level
    in a subsequent session (day_low <= poc <= day_high).
    """
    REPORTS_DIR.mkdir(exist_ok=True)
    frames = []

    for symbol, days in all_days.items():
        if days is None:
            continue

        df = days.select(["date", "poc", "day_high", "day_low", "day_type"]).sort("date")
        rows = df.to_dicts()
        n = len(rows)

        naked = []
        for i, row in enumerate(rows):
            poc = row["poc"]
            if poc is None:
                continue
            origin_date = row["date"]
            filled = False
            filled_date = None
            days_alive = 0

            # Look in following days whether the POC was hit
            for j in range(i + 1, n):
                future = rows[j]
                if future["day_low"] is not None and future["day_high"] is not None:
                    if future["day_low"] <= poc <= future["day_high"]:
                        filled = True
                        filled_date = future["date"]
                        days_alive = j - i
                        break

            if not filled:
                days_alive = n - 1 - i  # Still naked

            naked.append({
                "symbol": symbol,
                "origin_date": origin_date,
                "poc": poc,
                "day_type": row["day_type"],
                "filled": filled,
                "filled_date": filled_date,
                "days_alive": days_alive,
            })

        frames.append(pl.DataFrame(naked))

    if not frames:
        return

    combined = pl.concat(frames).sort(["symbol", "origin_date"])

    # Stats
    for symbol in ["GC", "NQ"]:
        sym_data = combined.filter(pl.col("symbol") == symbol)
        n_total = sym_data.shape[0]
        still_naked = sym_data.filter(pl.col("filled") == False)
        filled = sym_data.filter(pl.col("filled") == True)

        print(f"\n  ── Naked POCs: {symbol} ──")
        print(f"    Total POCs: {n_total}")
        print(f"    Still naked: {still_naked.shape[0]}")
        if filled.shape[0] > 0:
            avg_life = filled["days_alive"].mean()
            med_life = filled["days_alive"].median()
            print(f"    Filled: {filled.shape[0]} (avg duration {avg_life:.0f}d, med {med_life:.0f}d)")

            # Lifespan distribution
            d1 = filled.filter(pl.col("days_alive") <= 1).shape[0]
            d5 = filled.filter(pl.col("days_alive") <= 5).shape[0]
            d20 = filled.filter(pl.col("days_alive") <= 20).shape[0]
            print(f"    Filled in ≤1d: {d1} ({d1/filled.shape[0]*100:.0f}%) "
                  f" ≤5d: {d5} ({d5/filled.shape[0]*100:.0f}%) "
                  f" ≤20d: {d20} ({d20/filled.shape[0]*100:.0f}%)")

        # Top 10 oldest naked
        if still_naked.shape[0] > 0:
            oldest = still_naked.sort("days_alive", descending=True).head(10)
            print(f"    Top 10 oldest naked POCs:")
            for row in oldest.iter_rows(named=True):
                print(f"      {row['origin_date']}  POC {row['poc']:.2f}  "
                      f"({row['days_alive']}d)  type: {row['day_type']}")

    # Save CSV
    csv_path = REPORTS_DIR / "naked_poc.csv"
    combined.write_csv(csv_path)
    print(f"\n  Naked POC CSV: {csv_path}")
    print(f"    {combined.shape[0]} rows")


def main():
    print("=" * 65)
    print("  EAGLE — Market Profile Day Types")
    print("=" * 65)

    all_days = {}
    for symbol in ["GC", "NQ"]:
        all_days[symbol] = classify_days(symbol)

    save_reports(all_days)
    build_naked_pocs(all_days)
    print(f"\n  Done.")


if __name__ == "__main__":
    main()
