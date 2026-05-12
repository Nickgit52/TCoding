#!/usr/bin/env python3
"""
build_candles.py — Pre-aggregate ticks into 1m, 5m, 15m, 1H, Daily candles
Usage: python3 Scripts/build_candles.py

v3 — Roll by irreversible daily volume crossover.
     For each day, total volume per contract is computed.
     The roll happens when the new contract has more daily volume
     than the old one — and we never go back.
     Like Bloomberg/CQG for continuous contracts.

Reads from /Volumes/Sam128/TC_Sam128/Ticks_Parquet/, writes to Eagle/Data/Candles/
Streaming mode for NQ (268M ticks) — processes contract by contract.
"""
import polars as pl
from pathlib import Path
import gc

DATA_DIR = Path(__file__).parent.parent / "Data"    # Eagle/Data/
TICKS_DIR = Path("/Volumes/Sam128/TC_Sam128/Ticks_Parquet")  # Path B: absolute, on Sam128
OUT_DIR = DATA_DIR / "Candles"

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

SYMBOLS = {
    "GC": "GC_ticks.parquet",
    "NQ": "NQ_ticks.parquet",
}

TIMEFRAMES = {
    "1m":  "1m",
    "5m":  "5m",
    "15m": "15m",
    "1h":  "1h",
    "1d":  "1d",
}

STREAMING_THRESHOLD = 80_000_000


def build_roll_calendar(lf, symbol):
    """
    Build the roll calendar: for each day, which contract is front-month.
    Logic: irreversible daily volume crossover.

    1. Compute daily volume per contract
    2. For each day, the contract with the largest volume is the candidate
    3. Once a more recent contract takes the lead, we never go back

    Returns a DataFrame: [date, front_contract]
    """
    # CHRONOLOGICAL order from CONTRACT_CHAINS (not alphabetical!)
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

    # Make the roll irreversible: once we move to a "more recent" contract,
    # we never go back. Sort contracts by start date.
    contract_order = {c: i for i, c in enumerate(contracts)}

    dates = daily_winner["date"].to_list()
    winners = daily_winner["contract"].to_list()

    # Apply irreversibility
    front = winners[0]
    front_idx = contract_order.get(front, 0)
    roll_calendar = [(dates[0], front)]

    for i in range(1, len(dates)):
        candidate = winners[i]
        candidate_idx = contract_order.get(candidate, 0)

        if candidate_idx >= front_idx:
            # Same contract or more recent — OK
            front = candidate
            front_idx = candidate_idx
        # Otherwise keep the current front (irreversible)

        roll_calendar.append((dates[i], front))

    result = pl.DataFrame({
        "date": [r[0] for r in roll_calendar],
        "front_contract": [r[1] for r in roll_calendar],
    })

    # Print the rolls
    rolls = []
    prev = result["front_contract"][0]
    for row in result.iter_rows(named=True):
        if row["front_contract"] != prev:
            rolls.append((row["date"], prev, row["front_contract"]))
            prev = row["front_contract"]

    print(f"    Roll calendar: {len(rolls)} rolls over {result.shape[0]} days")
    for date, old, new in rolls:
        print(f"      {date.strftime('%Y-%m-%d')}: {old} → {new}")

    del daily_vol, daily_winner
    gc.collect()

    return result


def aggregate_ticks_to_candles(df, every):
    """Aggregate a tick DataFrame into candles."""
    return df.sort("datetime_utc").group_by_dynamic(
        "datetime_utc", every=every
    ).agg([
        pl.col("close").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume").sum().alias("volume"),
        pl.col("bid_vol").sum().alias("bid_vol"),
        pl.col("ask_vol").sum().alias("ask_vol"),
        pl.col("num_trades").sum().alias("num_trades"),
    ]).with_columns([
        (pl.col("ask_vol").cast(pl.Int64) - pl.col("bid_vol").cast(pl.Int64)).alias("delta"),
    ])


def build_symbol(symbol, tick_file):
    """Build all timeframes with an irreversible roll calendar."""
    path = TICKS_DIR / tick_file
    if not path.exists():
        print(f"  {symbol}: file not found ({path})")
        return

    lf = pl.scan_parquet(path)
    row_count = lf.select(pl.len()).collect().item()
    is_large = row_count > STREAMING_THRESHOLD
    mode = "streaming" if is_large else "standard"
    print(f"\n  {symbol}: {row_count:,} ticks — {mode} mode")

    # Step 1: build the roll calendar
    print(f"    Computing the roll calendar...")
    roll_cal = build_roll_calendar(lf, symbol)

    # Step 2: for each timeframe, aggregate front-month ticks only
    for tf_name, tf_every in TIMEFRAMES.items():
        print(f"    {tf_name}...", end=" ", flush=True)

        # Collect unique contracts from the calendar
        front_contracts = roll_cal["front_contract"].unique().sort().to_list()

        all_candles = []
        for contract in front_contracts:
            # Days when this contract is front
            front_dates = roll_cal.filter(
                pl.col("front_contract") == contract
            )["date"].to_list()

            if not front_dates:
                continue

            date_min = min(front_dates)
            date_max = max(front_dates) + pl.duration(days=1)  # include the last day

            # Filter ticks: this contract, within its front period
            contract_ticks = lf.filter(
                (pl.col("contract") == contract) &
                (pl.col("datetime_utc") >= date_min) &
                (pl.col("datetime_utc") < date_max)
            ).collect()

            if contract_ticks.shape[0] == 0:
                del contract_ticks
                continue

            candles = aggregate_ticks_to_candles(contract_ticks, tf_every)
            all_candles.append(candles)
            del contract_ticks
            gc.collect()

        if not all_candles:
            print("0 candles")
            continue

        candles = pl.concat(all_candles).sort("datetime_utc")
        del all_candles
        gc.collect()

        # Dedupe if overlap at the exact moment of the roll (same minute)
        if candles.select(pl.col("datetime_utc").is_duplicated().any()).item():
            candles = candles.group_by("datetime_utc").last().sort("datetime_utc")

        out_path = OUT_DIR / f"{symbol}_{tf_name}.parquet"
        candles.write_parquet(out_path, compression="zstd")
        size_mb = out_path.stat().st_size / 1e6
        print(f"{candles.shape[0]:,} candles ({size_mb:.1f} MB)")

        del candles
        gc.collect()

    del roll_cal
    gc.collect()


def verify_stitching():
    """Check stitching quality."""
    print(f"\n{'═'*65}")
    print(f"  Stitching verification")
    print(f"{'═'*65}")

    for symbol in SYMBOLS:
        for tf in ["1m", "1h", "1d"]:
            path = OUT_DIR / f"{symbol}_{tf}.parquet"
            if not path.exists():
                continue

            df = pl.read_parquet(path)
            if df.shape[0] < 2:
                continue

            returns = df.with_columns(
                ((pl.col("close") - pl.col("open")) / pl.col("open") * 100).alias("return_pct"),
            )

            ret = returns["return_pct"].drop_nulls()
            big_jumps = returns.filter(pl.col("return_pct").abs() > 5).shape[0]

            # Also check close-to-open gaps (inter-candle)
            close_to_open = df.with_columns(
                ((pl.col("open") - pl.col("close").shift(1)) / pl.col("close").shift(1) * 100).alias("gap_pct")
            )
            big_gaps = close_to_open.filter(pl.col("gap_pct").abs() > 2).shape[0]

            print(f"  {symbol} {tf:3s}: {df.shape[0]:,} candles | "
                  f"avg ret {ret.mean():+.4f}% | "
                  f"std {ret.std():.4f}% | "
                  f"intra jumps >5%: {big_jumps} | "
                  f"inter gaps >2%: {big_gaps}")

            del df, returns, close_to_open
            gc.collect()


def main():
    print("=" * 65)
    print("  EAGLE — Build Candles v3 (daily volume crossover)")
    print("=" * 65)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for symbol, tick_file in SYMBOLS.items():
        build_symbol(symbol, tick_file)
        gc.collect()
        print(f"  ✓ {symbol} done")

    verify_stitching()

    print(f"\n{'═'*65}")
    print(f"  Summary — {OUT_DIR}")
    print(f"{'═'*65}")
    for f in sorted(OUT_DIR.glob("*.parquet")):
        lf = pl.scan_parquet(f)
        rows = lf.select(pl.len()).collect().item()
        size_mb = f.stat().st_size / 1e6
        print(f"  {f.name:25s} {rows:>10,} candles  {size_mb:>7.1f} MB")

    print(f"\n  Done.")


if __name__ == "__main__":
    main()
