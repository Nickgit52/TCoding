#!/usr/bin/env python3
"""
explore_ml.py — Quality control & ML exploration (v4)
Usage: python3 Scripts/explore_ml.py

v4 — Full analysis: ticks (front-month) + pre-aggregated candles.
     Tick-level continuity with front-month stitching.
     Tick microstructure: trade size, inter-arrival, order flow.
"""
import polars as pl
from pathlib import Path
from datetime import timedelta, datetime
import gc
import sys

DATA_DIR = Path(__file__).parent.parent / "Data"    # Eagle/Data/
TICKS_DIR = Path("/Volumes/Sam128/TC_Sam128/Ticks_Parquet")  # Path B: absolute, on Sam128
CANDLES_DIR = DATA_DIR / "Candles"
REPORTS_DIR = DATA_DIR / "Reports"

# Chronological chains — oldest to most recent
# MUST match the order in build_history.py
CONTRACT_CHAINS = {
    "GC": [
        "GCG24-COMEX", "GCQ24-COMEX", "GCZ24-COMEX",
        "GCG25-COMEX", "GCJ25-COMEX", "GCM25-COMEX",
        "GCQ25-COMEX", "GCG26-COMEX", "GCJ26-COMEX", "GCM26-COMEX",
    ],
    "NQ": [
        "NQH24-CME", "NQM24-CME", "NQU24-CME", "NQZ24-CME",
        "NQH25-CME", "NQM25-CME", "NQU25-CME", "NQZ25-CME",
        "NQH26-CME", "NQM26-CME",
    ],
}

SYMBOLS = ["GC", "NQ"]
STREAMING_THRESHOLD = 80_000_000


class TeeWriter:
    """Write simultaneously to the terminal and to a file."""
    def __init__(self, file_handle, original_stdout):
        self.file = file_handle
        self.stdout = original_stdout

    def write(self, text):
        self.stdout.write(text)
        self.file.write(text)

    def flush(self):
        self.stdout.flush()
        self.file.flush()


def section(title):
    print(f"\n{'═'*65}")
    print(f"  {title}")
    print(f"{'═'*65}")


def subsection(title):
    print(f"\n  ── {title} ──")


# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY CONTROL (tick-level, lazy)
# ═══════════════════════════════════════════════════════════════════════════════

def quality_control(symbol):
    """QC on ticks — lazy mode."""
    path = TICKS_DIR / f"{symbol}_ticks.parquet"
    if not path.exists():
        print(f"  {symbol} ticks: file not found")
        return

    section(f"{symbol} — Quality Control (ticks)")
    lf = pl.scan_parquet(path)

    row_count = lf.select(pl.len()).collect().item()
    file_mb = path.stat().st_size / 1e6
    print(f"\n  Ticks: {row_count:,}  |  File: {file_mb:.0f} MB")

    stats = lf.select(
        pl.col("datetime_utc").min().alias("dt_min"),
        pl.col("datetime_utc").max().alias("dt_max"),
    ).collect()
    dt_min, dt_max = stats["dt_min"][0], stats["dt_max"][0]
    days = (dt_max - dt_min).days
    print(f"  {dt_min} → {dt_max} ({days}d / {days/30:.0f} months)")

    null_counts = lf.null_count().collect()
    columns = lf.collect_schema().names()
    null_cols = [c for c in columns if null_counts[c][0] > 0]
    print(f"  Nulls    : {'⚠ ' + str(null_cols) if null_cols else '✓ none'}")

    zero_vol = lf.filter(pl.col("volume") == 0).select(pl.len()).collect().item()
    print(f"  Vol=0    : {'⚠ ' + str(zero_vol) if zero_vol else '✓ none'}")

    price_stats = lf.select(
        pl.col("close").median().alias("med"),
        pl.col("close").std().alias("std"),
    ).collect()
    med, std = price_stats["med"][0], price_stats["std"][0]
    outliers = lf.filter(
        (pl.col("close") < med - 10 * std) |
        (pl.col("close") > med + 10 * std) |
        (pl.col("close") <= 0)
    ).select(pl.len()).collect().item()
    print(f"  Outliers : {'⚠ ' + str(outliers) if outliers else '✓ none'}")

    if row_count > STREAMING_THRESHOLD:
        sample = lf.head(5_000_000).collect()
        dupes = sample.shape[0] - sample.unique().shape[0]
        print(f"  Dupes    : {'⚠ ' + str(dupes) if dupes else '✓ none'} (5M sample)")
        del sample
    else:
        df = lf.collect()
        dupes = df.shape[0] - df.unique().shape[0]
        print(f"  Dupes    : {'⚠ ' + str(dupes) if dupes else '✓ none'}")
        del df

    gc.collect()


# ═══════════════════════════════════════════════════════════════════════════════
# ROLL ANALYSIS (tick-level, lazy)
# ═══════════════════════════════════════════════════════════════════════════════

def roll_analysis(symbol):
    """Roll analysis — lazy."""
    path = TICKS_DIR / f"{symbol}_ticks.parquet"
    if not path.exists():
        return

    section(f"{symbol} — Roll Analysis")
    lf = pl.scan_parquet(path)

    stats = lf.group_by("contract").agg([
        pl.col("datetime_utc").min().alias("start"),
        pl.col("datetime_utc").max().alias("end"),
        pl.len().alias("ticks"),
        pl.col("volume").sum().alias("total_vol"),
        pl.col("close").mean().alias("avg_price"),
    ]).sort("start").collect()

    print(f"\n  Contracts: {stats.shape[0]}")
    for row in stats.iter_rows(named=True):
        days = (row["end"] - row["start"]).days
        print(f"  {row['contract']:20s} {row['start'].strftime('%Y-%m-%d')} → {row['end'].strftime('%Y-%m-%d')} ({days:3d}d) {row['ticks']:>12,} ticks  avg {row['avg_price']:.2f}")

    contract_list = stats.sort("start").to_dicts()
    overlaps = 0
    for i in range(len(contract_list) - 1):
        c1, c2 = contract_list[i], contract_list[i + 1]
        if c2["start"] < c1["end"]:
            overlaps += 1
            overlap_days = (c1["end"] - c2["start"]).days
            print(f"    ↔ {c1['contract'].strip():15s} ↔ {c2['contract'].strip():15s}: {overlap_days}d overlap")
    if overlaps == 0:
        print(f"  No overlap")

    del stats
    gc.collect()


# ═══════════════════════════════════════════════════════════════════════════════
# TICK CONTINUITY — Front-month stitching at the tick level
# ═══════════════════════════════════════════════════════════════════════════════

def tick_continuity(symbol):
    """
    Analyze tick continuity with irreversible roll calendar.
    Daily volume crossover: when the new contract has more daily volume than
    the old one, we roll and never go back.
    Measures price jumps at each roll.
    """
    path = TICKS_DIR / f"{symbol}_ticks.parquet"
    if not path.exists():
        return

    section(f"{symbol} — Tick Continuity (irreversible roll)")
    lf = pl.scan_parquet(path)

    # Step 1: daily volume per contract
    print(f"\n  Computing daily volume per contract...")
    # CHRONOLOGICAL order (not alphabetical!)
    contracts = CONTRACT_CHAINS[symbol]

    all_daily = []
    for contract in contracts:
        daily = lf.filter(
            pl.col("contract") == contract
        ).with_columns(
            pl.col("datetime_utc").dt.truncate("1d").alias("date")
        ).group_by("date").agg([
            pl.col("volume").sum().alias("vol"),
            pl.lit(contract).alias("contract"),
        ]).collect()
        all_daily.append(daily)
        gc.collect()

    daily_vol = pl.concat(all_daily).sort(["date", "vol"], descending=[False, True])
    del all_daily
    gc.collect()

    # For each day, the contract with the largest volume
    daily_winner = daily_vol.group_by("date").first().sort("date")
    total_days = daily_winner.shape[0]

    # Make the roll irreversible
    contract_order = {c: i for i, c in enumerate(contracts)}
    dates = daily_winner["date"].to_list()
    winners = daily_winner["contract"].to_list()

    front = winners[0]
    front_idx = contract_order.get(front, 0)
    calendar = [(dates[0], front)]

    for i in range(1, len(dates)):
        candidate = winners[i]
        candidate_idx = contract_order.get(candidate, 0)
        if candidate_idx >= front_idx:
            front = candidate
            front_idx = candidate_idx
        calendar.append((dates[i], front))

    # Identify rolls
    transitions = []
    prev_contract = calendar[0][1]
    for date, contract in calendar[1:]:
        if contract != prev_contract:
            transitions.append({"date": date, "from": prev_contract, "to": contract})
            prev_contract = contract
        else:
            prev_contract = contract

    print(f"  Total days: {total_days:,}")
    print(f"  Irreversible rolls: {len(transitions)}")

    del daily_vol, daily_winner
    gc.collect()

    # Step 2: measure the price jump at each roll
    if transitions:
        subsection("Roll details (price jumps)")

        for t in transitions:
            # Last tick of the outgoing contract before the roll day
            last_price_old = lf.filter(
                (pl.col("contract") == t["from"]) &
                (pl.col("datetime_utc") < t["date"])
            ).sort("datetime_utc").select(
                pl.col("close").last()
            ).collect().item()

            # First tick of the incoming contract on the roll day
            first_price_new = lf.filter(
                (pl.col("contract") == t["to"]) &
                (pl.col("datetime_utc") >= t["date"])
            ).sort("datetime_utc").select(
                pl.col("close").first()
            ).collect().item()

            if last_price_old and first_price_new:
                jump = first_price_new - last_price_old
                jump_pct = jump / last_price_old * 100
                flag = " ⚠" if abs(jump_pct) > 1.5 else ""
                print(f"    {t['date'].strftime('%Y-%m-%d')}: "
                      f"{t['from']:20s} → {t['to']:20s} | "
                      f"jump {jump:+.2f} ({jump_pct:+.3f}%){flag}")

    # Step 3: calendar stats
    subsection("Front-month duration per contract")
    cal_df = pl.DataFrame({
        "date": [c[0] for c in calendar],
        "contract": [c[1] for c in calendar],
    })
    usage = cal_df.group_by("contract").agg(
        pl.len().alias("days")
    ).sort("days", descending=True)

    for row in usage.iter_rows(named=True):
        pct = row["days"] / total_days * 100
        bar = "█" * int(pct / 100 * 40)
        print(f"    {row['contract']:20s} {row['days']:>5}d ({pct:5.1f}%)  {bar}")

    del cal_df
    gc.collect()


# ═══════════════════════════════════════════════════════════════════════════════
# TICK MICROSTRUCTURE (front-month, streaming)
# ═══════════════════════════════════════════════════════════════════════════════

def tick_microstructure(symbol):
    """Tick-level microstructure analysis — trade size, inter-arrival, order flow."""
    path = TICKS_DIR / f"{symbol}_ticks.parquet"
    if not path.exists():
        return

    section(f"{symbol} — Tick Microstructure")
    lf = pl.scan_parquet(path)

    # Use a recent sample (last front-month contract = most relevant)
    # Find the contract with the most recent volume
    contracts = lf.group_by("contract").agg(
        pl.col("volume").sum().alias("total_vol"),
        pl.col("datetime_utc").max().alias("last_tick"),
    ).sort("last_tick", descending=True).collect()

    front = contracts["contract"][0]
    print(f"\n  Contract analyzed: {front} (current front-month)")

    # Load the front-month
    df = lf.filter(pl.col("contract") == front).sort("datetime_utc").collect()
    n = df.shape[0]
    print(f"  Ticks: {n:,}")

    # Trade size distribution
    subsection("Trade size distribution (volume per tick)")
    vol = df["volume"]
    print(f"    Mean    : {vol.mean():.1f}")
    print(f"    Median  : {vol.median():.0f}")
    print(f"    Mode    : {vol.mode().to_list()[0] if vol.mode().len() > 0 else 'N/A'}")
    print(f"    P90     : {vol.quantile(0.90):.0f}")
    print(f"    P99     : {vol.quantile(0.99):.0f}")
    print(f"    Max     : {vol.max()}")

    # Lot sizes
    for threshold in [1, 5, 10, 50, 100]:
        count = (vol >= threshold).sum()
        pct = count / n * 100
        print(f"    ≥{threshold:>3} lots: {count:>10,} ({pct:.1f}%)")

    # Inter-arrival time
    subsection("Inter-arrival time")
    # Sample to avoid OOM on diffs
    sample_n = min(n, 2_000_000)
    sample = df.head(sample_n)
    diffs = sample.with_columns(
        pl.col("datetime_utc").diff().dt.total_microseconds().alias("dt_us")
    ).filter(pl.col("dt_us").is_not_null())

    dt_us = diffs["dt_us"]
    dt_ms = dt_us / 1000
    print(f"    (sample: {sample_n:,} ticks)")
    print(f"    Mean    : {dt_ms.mean():.1f} ms")
    print(f"    Median  : {dt_ms.median():.1f} ms")
    print(f"    P10     : {dt_ms.quantile(0.10):.1f} ms")
    print(f"    P90     : {dt_ms.quantile(0.90):.1f} ms")
    print(f"    P99     : {dt_ms.quantile(0.99):.1f} ms")

    # % of simultaneous ticks (same millisecond)
    zero_dt = (dt_us == 0).sum()
    pct_zero = zero_dt / dt_us.len() * 100
    print(f"    Simultaneous (0ms): {zero_dt:,} ({pct_zero:.1f}%)")

    del diffs, sample
    gc.collect()

    # Order flow — bid/ask imbalance
    subsection("Order flow (bid/ask)")
    total_bid = df["bid_vol"].sum()
    total_ask = df["ask_vol"].sum()
    total_all = total_bid + total_ask
    imbalance = (total_ask - total_bid) / total_all * 100 if total_all > 0 else 0
    print(f"    Bid total : {total_bid:>15,}")
    print(f"    Ask total : {total_ask:>15,}")
    print(f"    Imbalance : {imbalance:+.2f}% ({'buyers' if imbalance > 0 else 'sellers'})")

    # Imbalance per hour
    subsection("Hourly imbalance (ask-bid)/total %")
    hourly = df.with_columns(
        pl.col("datetime_utc").dt.hour().alias("hour")
    ).group_by("hour").agg([
        pl.col("bid_vol").sum().alias("bid"),
        pl.col("ask_vol").sum().alias("ask"),
    ]).with_columns(
        ((pl.col("ask") - pl.col("bid")) / (pl.col("ask") + pl.col("bid")) * 100).alias("imbal")
    ).sort("hour")

    for row in hourly.iter_rows(named=True):
        val = row["imbal"]
        bar_pos = "+" * int(max(0, val) * 5)
        bar_neg = "-" * int(max(0, -val) * 5)
        print(f"    {row['hour']:02d}h: {val:+5.2f}%  {bar_neg}{bar_pos}")

    # Price impact — correlation between tick-by-tick delta and return
    subsection("Price impact (delta vs return)")
    impact_sample = df.head(min(n, 500_000)).with_columns([
        (pl.col("close").diff()).alias("ret"),
        (pl.col("ask_vol").cast(pl.Int64) - pl.col("bid_vol").cast(pl.Int64)).alias("tick_delta"),
    ]).drop_nulls(subset=["ret", "tick_delta"])

    if impact_sample.shape[0] > 100:
        # Correlation via DataFrame (compatible with all Polars versions)
        corr = impact_sample.select(
            pl.corr("ret", "tick_delta", method="pearson")
        ).item()
        print(f"    Delta-return correlation: {corr:.4f}")
        print(f"    {'→ tradable signal' if abs(corr) > 0.05 else '→ weak'}")

        # Mean impact on large delta
        big_buy = impact_sample.filter(pl.col("tick_delta") > impact_sample["tick_delta"].quantile(0.95))
        big_sell = impact_sample.filter(pl.col("tick_delta") < impact_sample["tick_delta"].quantile(0.05))
        if big_buy.shape[0] > 0 and big_sell.shape[0] > 0:
            print(f"    Avg return on large buy (P95 delta) : {big_buy['ret'].mean():+.4f}")
            print(f"    Avg return on large sell (P5 delta) : {big_sell['ret'].mean():+.4f}")

    del df, impact_sample
    gc.collect()


# ═══════════════════════════════════════════════════════════════════════════════
# CANDLE ANALYSIS (fast, from pre-aggregated)
# ═══════════════════════════════════════════════════════════════════════════════

def load_candles(symbol, tf):
    path = CANDLES_DIR / f"{symbol}_{tf}.parquet"
    if not path.exists():
        return None
    return pl.read_parquet(path)


def basic_stats(symbol):
    """Basic multi-timeframe stats (stitched candles)."""
    section(f"{symbol} — Candle Stats (multi-timeframe)")

    for tf in ["1m", "5m", "15m", "1h", "1d"]:
        df = load_candles(symbol, tf)
        if df is None:
            continue

        df = df.with_columns([
            (pl.col("close") - pl.col("open")).alias("return"),
            (pl.col("high") - pl.col("low")).alias("range"),
        ])

        returns = df["return"].drop_nulls()
        vols = df["volume"].drop_nulls()

        subsection(f"{tf} — {df.shape[0]:,} candles")
        print(f"    Return : avg {returns.mean():.4f}  std {returns.std():.4f}  skew {returns.skew():.2f}  kurt {returns.kurtosis():.1f}")
        print(f"    Range  : avg {df['range'].mean():.2f}  med {df['range'].median():.2f}")
        print(f"    Volume : avg {vols.mean():.0f}  med {vols.median():.0f}  P95 {vols.quantile(0.95):.0f}")
        print(f"    Delta  : avg {df['delta'].mean():.1f}  % pos {(df['delta'] > 0).sum() / df.shape[0] * 100:.1f}%")

        del df
        gc.collect()


def autocorrelation_analysis(symbol):
    """Return autocorrelation."""
    section(f"{symbol} — Autocorrelation")

    for tf in ["1m", "5m", "15m", "1h"]:
        df = load_candles(symbol, tf)
        if df is None:
            continue

        returns = df.with_columns(
            (pl.col("close") - pl.col("open")).alias("ret")
        )["ret"].drop_nulls()

        n = returns.len()
        if n < 100:
            continue

        ret_list = returns.to_list()
        mean_r = returns.mean()
        var_r = returns.var()
        if var_r == 0:
            continue

        lags_str = []
        for lag in [1, 2, 3, 5, 10]:
            if lag >= n:
                break
            autocorr = sum(
                (ret_list[i] - mean_r) * (ret_list[i - lag] - mean_r)
                for i in range(lag, min(n, 50000))
            ) / (min(n, 50000) - lag) / var_r
            lags_str.append(f"lag{lag}={autocorr:+.4f}")

        regime = "mean-reversion" if float(lags_str[0].split("=")[1]) < -0.02 else "momentum" if float(lags_str[0].split("=")[1]) > 0.02 else "neutral"
        print(f"  {tf:4s}: {', '.join(lags_str)}  → {regime}")

        del df
        gc.collect()


def volatility_analysis(symbol):
    """Volatility regimes."""
    section(f"{symbol} — Volatility")

    df = load_candles(symbol, "1h")
    if df is None:
        return

    df = df.with_columns([
        (pl.col("high") - pl.col("low")).alias("range"),
    ])

    df = df.with_columns([
        pl.col("range").rolling_mean(20).alias("vol_20"),
        pl.col("range").rolling_mean(100).alias("vol_100"),
    ])

    clean = df.drop_nulls(subset=["vol_20", "vol_100"])
    if clean.shape[0] == 0:
        return

    median_vol = clean["vol_20"].median()
    high_vol = clean.filter(pl.col("vol_20") > median_vol * 1.5)
    low_vol = clean.filter(pl.col("vol_20") < median_vol * 0.6)

    subsection("Volatility regimes (1h, rolling 20)")
    print(f"    Median 1h range  : {median_vol:.2f}")
    print(f"    High vol (>1.5x) : {high_vol.shape[0]:,} candles ({high_vol.shape[0]/clean.shape[0]*100:.1f}%)")
    print(f"    Low vol  (<0.6x) : {low_vol.shape[0]:,} candles ({low_vol.shape[0]/clean.shape[0]*100:.1f}%)")

    ranges = clean["range"].to_list()
    n = len(ranges)
    mean_rng = clean["range"].mean()
    var_rng = clean["range"].var()
    if var_rng > 0 and n > 100:
        ac1 = sum(
            (ranges[i] - mean_rng) * (ranges[i-1] - mean_rng)
            for i in range(1, min(n, 10000))
        ) / (min(n, 10000) - 1) / var_rng
        print(f"    Range autocorr(1): {ac1:.4f} {'— strong persistence' if ac1 > 0.3 else '— moderate' if ac1 > 0.1 else ''}")

    subsection("Average range per hour (UTC)")
    hourly = clean.with_columns(
        pl.col("datetime_utc").dt.hour().alias("hour")
    ).group_by("hour").agg(
        pl.col("range").mean().alias("avg_range"),
        pl.col("volume").mean().alias("avg_vol"),
    ).sort("hour")

    for row in hourly.iter_rows(named=True):
        bar = "█" * int(row["avg_range"] / hourly["avg_range"].max() * 30)
        print(f"    {row['hour']:02d}h: range {row['avg_range']:>8.2f}  vol {row['avg_vol']:>8.0f}  {bar}")

    del df, clean
    gc.collect()


def daily_patterns(symbol):
    """Daily patterns."""
    section(f"{symbol} — Daily Patterns")

    df = load_candles(symbol, "1d")
    if df is None or df.shape[0] < 30:
        return

    df = df.with_columns([
        (pl.col("close") - pl.col("open")).alias("return"),
        (pl.col("high") - pl.col("low")).alias("range"),
        ((pl.col("close") - pl.col("open")) / pl.col("open") * 100).alias("return_pct"),
    ])

    ret_pct = df["return_pct"].drop_nulls()
    subsection(f"Daily returns ({df.shape[0]} days)")
    print(f"    Mean    : {ret_pct.mean():+.3f}%")
    print(f"    Std     : {ret_pct.std():.3f}%")
    print(f"    Min     : {ret_pct.min():+.2f}%")
    print(f"    Max     : {ret_pct.max():+.2f}%")
    print(f"    % positive: {(ret_pct > 0).sum() / ret_pct.len() * 100:.1f}%")

    subsection("Average return per weekday")
    weekly = df.with_columns(
        pl.col("datetime_utc").dt.weekday().alias("dow")
    ).group_by("dow").agg([
        pl.col("return_pct").mean().alias("avg_ret"),
        pl.col("range").mean().alias("avg_range"),
        pl.col("volume").mean().alias("avg_vol"),
        pl.len().alias("count"),
    ]).sort("dow")

    day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    for row in weekly.iter_rows(named=True):
        name = day_names.get(row["dow"], "?")
        bar_ret = "+" * int(max(0, row["avg_ret"]) * 20) + "-" * int(max(0, -row["avg_ret"]) * 20)
        print(f"    {name}: ret {row['avg_ret']:+.3f}%  range {row['avg_range']:.2f}  vol {row['avg_vol']:.0f}  (n={row['count']})  {bar_ret}")

    rets = df["return_pct"].drop_nulls().to_list()
    streaks_up, streaks_dn = [], []
    current = 0
    for r in rets:
        if r > 0:
            if current > 0:
                current += 1
            else:
                if current < 0:
                    streaks_dn.append(abs(current))
                current = 1
        elif r < 0:
            if current < 0:
                current -= 1
            else:
                if current > 0:
                    streaks_up.append(current)
                current = -1
    if current > 0:
        streaks_up.append(current)
    elif current < 0:
        streaks_dn.append(abs(current))

    if streaks_up and streaks_dn:
        subsection("Consecutive streaks")
        print(f"    Up   : max {max(streaks_up)}d  avg {sum(streaks_up)/len(streaks_up):.1f}d")
        print(f"    Down : max {max(streaks_dn)}d  avg {sum(streaks_dn)/len(streaks_dn):.1f}d")

    del df
    gc.collect()


def feature_ideas(symbol):
    """Suggested ML features."""
    section(f"{symbol} — Suggested ML features")

    ideas = [
        "volume_ratio_5m    : volume / MA(20) volume — activity peaks",
        "delta_cumul_15m    : cumulative delta over 15min — directional pressure",
        "range_vs_vol       : range / MA(20) range — expansion/contraction",
        "hour_of_day        : UTC hour — strong intraday seasonality",
        "day_of_week        : weekday — weekly patterns",
        "ret_autocorr_5     : 5-lag autocorrelation — momentum/mean-reversion",
        "vol_regime         : vol_20 / vol_100 — volatility regime",
        "bid_ask_imbalance  : (ask-bid)/(ask+bid) — order flow",
        "trade_size_ratio   : tick vol / median — detects large orders",
        "inter_arrival_z    : time between ticks / avg — market acceleration",
        "price_impact       : delta→return correlation — directional liquidity",
    ]

    for idea in ideas:
        print(f"  • {idea}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"ml_report_{timestamp}.txt"

    original_stdout = sys.stdout
    report_file = open(report_path, "w", encoding="utf-8")
    sys.stdout = TeeWriter(report_file, original_stdout)

    try:
        print("=" * 65)
        print(f"  EAGLE — ML Explorer v4 (ticks + candles)")
        print(f"  Report: {report_path.name}")
        print(f"  Date  : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 65)

        has_candles = CANDLES_DIR.exists() and list(CANDLES_DIR.glob("*.parquet"))
        has_ticks = TICKS_DIR.exists() and list(TICKS_DIR.glob("*.parquet"))

        if not has_ticks:
            print("\n  ⚠ No ticks in Data/Ticks_Parquet/")
            print("  → Run first: python3 build_history.py")
            return

        if not has_candles:
            print("\n  ⚠ No candles in Data/Candles/")
            print("  → Run first: python3 build_candles.py")
            print("  → Tick-only analysis...\n")

        for symbol in SYMBOLS:
            # === TICK ANALYSIS ===
            quality_control(symbol)
            roll_analysis(symbol)
            tick_continuity(symbol)
            tick_microstructure(symbol)

            # === CANDLE ANALYSIS ===
            if has_candles:
                basic_stats(symbol)
                autocorrelation_analysis(symbol)
                volatility_analysis(symbol)
                daily_patterns(symbol)

            feature_ideas(symbol)
            gc.collect()
            print(f"\n  {'═'*40}")
            print(f"  ✓ {symbol} done")
            print(f"  {'═'*40}")

        print(f"\n  Report saved: {report_path}")

    finally:
        sys.stdout = original_stdout
        report_file.close()
        print(f"\n  📄 Report: Data/Reports/{report_path.name}")


if __name__ == "__main__":
    main()
