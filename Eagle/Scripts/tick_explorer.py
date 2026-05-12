#!/usr/bin/env python3
"""
tick_explorer.py — Quick tick exploration from the Parquet files
Runs on MAC — Reads /Volumes/Sam128/TC_Sam128/Ticks_Parquet/

Usage:
    cd ~/Documents/Projets/TCoding/Eagle

    # Last 30 days (default)
    python3 Scripts/tick_explorer.py GC
    python3 Scripts/tick_explorer.py NQ

    # Full history
    python3 Scripts/tick_explorer.py GC --all

    # Last 7 days
    python3 Scripts/tick_explorer.py NQ --last 7

    # Filter a specific contract
    python3 Scripts/tick_explorer.py GC --contract GCJ26-COMEX

    # Filter by date (disables --last)
    python3 Scripts/tick_explorer.py NQ --from 2026-03-10 --to 2026-03-14

    # Filter by UTC hour (e.g. RTH only)
    python3 Scripts/tick_explorer.py GC --hours 13-22

    # Export a CSV sample (top N ticks by volume)
    python3 Scripts/tick_explorer.py GC --contract GCJ26-COMEX --export top500

    # Export a full range to CSV
    python3 Scripts/tick_explorer.py NQ --from 2026-03-14 --export all

    # Combine filters
    python3 Scripts/tick_explorer.py GC --contract GCJ26-COMEX --last 7 --hours 13-16 --export all
"""
import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("Install polars: pip3 install polars")
    exit(1)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent.parent
TICK_DIR = Path("/Volumes/Sam128/TC_Sam128/Ticks_Parquet")  # Path B: absolute, on Sam128
EXPORT_DIR = BASE_DIR / "Data" / "Tick_Exports"

GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def load_ticks(symbol):
    """Load the tick parquet for a symbol."""
    path = TICK_DIR / f"{symbol}_ticks.parquet"
    if not path.exists():
        print(f"  File not found: {path}")
        print(f"  Run first: python3 Scripts/build_history.py")
        sys.exit(1)

    size_mb = os.path.getsize(path) / 1e6
    print(f"  Loading {path.name} ({size_mb:.0f} MB)...", end=" ", flush=True)
    df = pl.read_parquet(path)
    print(f"{df.shape[0]:,} ticks")
    return df


def apply_filters(df, args):
    """Apply requested filters."""
    original = df.shape[0]

    # --last N days filter (default 30, unless --from/--to/--all)
    if not args.all_data and not args.date_from and not args.date_to:
        cutoff = datetime.utcnow() - timedelta(days=args.last)
        df = df.filter(pl.col("datetime_utc") >= cutoff)
        if df.shape[0] < original:
            print(f"  --last {args.last}d: {original:,} → {df.shape[0]:,} ticks")
            original = df.shape[0]

    # Contract filter
    if args.contract:
        available = df.select("contract").unique().to_series().to_list()
        df = df.filter(pl.col("contract") == args.contract)
        if df.shape[0] == 0:
            print(f"  Contract '{args.contract}' not found.")
            print(f"  Available contracts: {', '.join(sorted(available))}")
            sys.exit(1)

    # Date filter
    if args.date_from:
        dt = datetime.strptime(args.date_from, "%Y-%m-%d")
        df = df.filter(pl.col("datetime_utc") >= dt)
    if args.date_to:
        dt = datetime.strptime(args.date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        df = df.filter(pl.col("datetime_utc") <= dt)

    # UTC hour filter
    if args.hours:
        parts = args.hours.split("-")
        h_start, h_end = int(parts[0]), int(parts[1])
        df = df.filter(
            (pl.col("datetime_utc").dt.hour() >= h_start) &
            (pl.col("datetime_utc").dt.hour() < h_end)
        )

    filtered = df.shape[0]
    if filtered < original:
        print(f"  Filtered: {original:,} → {filtered:,} ticks")

    return df


def print_overview(df, symbol):
    """Print a global summary."""
    contracts = df.select("contract").unique().sort("contract").to_series().to_list()
    first = df["datetime_utc"].min()
    last = df["datetime_utc"].max()
    days = (last - first).days

    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"  {CYAN}{symbol}{RESET} — {df.shape[0]:,} ticks")
    print(f"  Range: {first.strftime('%Y-%m-%d %H:%M')} → {last.strftime('%Y-%m-%d %H:%M')} ({days}d)")
    print(f"  Contracts ({len(contracts)}): {', '.join(contracts)}")
    print(f"{'─'*60}")


def print_contract_stats(df):
    """Per-contract stats."""
    stats = (
        df.group_by("contract")
        .agg([
            pl.len().alias("ticks"),
            pl.col("datetime_utc").min().alias("first"),
            pl.col("datetime_utc").max().alias("last"),
            pl.col("close").min().alias("low"),
            pl.col("close").max().alias("high"),
            pl.col("volume").sum().alias("total_vol"),
            pl.col("bid_vol").sum().cast(pl.Int64).alias("total_bid"),
            pl.col("ask_vol").sum().cast(pl.Int64).alias("total_ask"),
        ])
        .sort("first")
    )

    print(f"\n  {BOLD}Per contract:{RESET}")
    print(f"  {'Contract':<18} {'Ticks':>12} {'From':>12} {'To':>12} {'Low':>10} {'High':>10} {'Delta':>12}")
    print(f"  {'─'*88}")

    for row in stats.iter_rows(named=True):
        delta = row["total_ask"] - row["total_bid"]
        delta_str = f"+{delta:,}" if delta > 0 else f"{delta:,}"
        print(
            f"  {row['contract']:<18} "
            f"{row['ticks']:>12,} "
            f"{row['first'].strftime('%Y-%m-%d'):>12} "
            f"{row['last'].strftime('%Y-%m-%d'):>12} "
            f"{row['low']:>10.2f} "
            f"{row['high']:>10.2f} "
            f"{delta_str:>12}"
        )


def print_daily_stats(df, limit=20):
    """Per-day stats (last N days)."""
    daily = (
        df.with_columns(pl.col("datetime_utc").dt.date().alias("date"))
        .group_by("date")
        .agg([
            pl.len().alias("ticks"),
            pl.col("close").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            (pl.col("ask_vol").sum().cast(pl.Int64) - pl.col("bid_vol").sum().cast(pl.Int64)).alias("delta"),
            pl.col("num_trades").sum().alias("trades"),
        ])
        .sort("date", descending=True)
        .head(limit)
    )

    print(f"\n  {BOLD}Last {min(limit, daily.shape[0])} days:{RESET}")
    print(f"  {'Date':>12} {'Ticks':>10} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume':>12} {'Delta':>10}")
    print(f"  {'─'*96}")

    for row in daily.iter_rows(named=True):
        delta = row["delta"]
        delta_str = f"+{delta:,}" if delta > 0 else f"{delta:,}"
        print(
            f"  {str(row['date']):>12} "
            f"{row['ticks']:>10,} "
            f"{row['open']:>10.2f} "
            f"{row['high']:>10.2f} "
            f"{row['low']:>10.2f} "
            f"{row['close']:>10.2f} "
            f"{row['volume']:>12,} "
            f"{delta_str:>10}"
        )


def print_big_trades(df, top_n=20):
    """The N largest trades (by volume)."""
    biggest = df.sort("volume", descending=True).head(top_n)

    print(f"\n  {BOLD}Top {top_n} largest trades:{RESET}")
    print(f"  {'DateTime UTC':>22} {'Contract':<18} {'Price':>10} {'Volume':>10} {'BidV':>8} {'AskV':>8} {'Delta':>8}")
    print(f"  {'─'*96}")

    for row in biggest.iter_rows(named=True):
        delta = int(row["ask_vol"]) - int(row["bid_vol"])
        delta_str = f"+{delta}" if delta > 0 else str(delta)
        print(
            f"  {row['datetime_utc'].strftime('%Y-%m-%d %H:%M:%S'):>22} "
            f"{row['contract']:<18} "
            f"{row['close']:>10.2f} "
            f"{row['volume']:>10,} "
            f"{row['bid_vol']:>8,} "
            f"{row['ask_vol']:>8,} "
            f"{delta_str:>8}"
        )


def print_hourly_profile(df):
    """Hourly profile — volume and activity by UTC hour."""
    hourly = (
        df.with_columns(pl.col("datetime_utc").dt.hour().alias("hour"))
        .group_by("hour")
        .agg([
            pl.len().alias("ticks"),
            pl.col("volume").sum().alias("volume"),
            pl.col("volume").mean().alias("avg_vol"),
            (pl.col("ask_vol").sum().cast(pl.Int64) - pl.col("bid_vol").sum().cast(pl.Int64)).alias("delta"),
        ])
        .sort("hour")
    )

    max_vol = hourly["volume"].max()

    print(f"\n  {BOLD}Hourly profile (UTC):{RESET}")
    print(f"  {'Hour':>6} {'Ticks':>12} {'Volume':>14} {'Avg Vol':>10} {'Delta':>12} {'Bar'}")
    print(f"  {'─'*80}")

    for row in hourly.iter_rows(named=True):
        delta = row["delta"]
        delta_str = f"+{delta:,}" if delta > 0 else f"{delta:,}"
        bar_len = int(30 * row["volume"] / max_vol) if max_vol > 0 else 0
        bar = "█" * bar_len
        print(
            f"  {row['hour']:>5}h "
            f"{row['ticks']:>12,} "
            f"{row['volume']:>14,} "
            f"{row['avg_vol']:>10.0f} "
            f"{delta_str:>12} "
            f" {bar}"
        )


def export_ticks(df, symbol, mode, args):
    """Export filtered ticks to CSV."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    if mode == "all":
        export_df = df
        suffix = "all"
    else:
        # topN — the N largest by volume
        n = int(mode.replace("top", ""))
        export_df = df.sort("volume", descending=True).head(n)
        suffix = f"top{n}"

    # File name
    parts = [symbol, suffix]
    if args.contract:
        parts.insert(1, args.contract.replace("-", "_"))
    if args.date_from:
        parts.append(f"from{args.date_from}")
    if args.date_to:
        parts.append(f"to{args.date_to}")
    if args.hours:
        parts.append(f"h{args.hours}")

    filename = "_".join(parts) + ".csv"
    out_path = EXPORT_DIR / filename

    export_df.write_csv(out_path)
    size_mb = os.path.getsize(out_path) / 1e6
    print(f"\n  {GREEN}Exported: {out_path}{RESET}")
    print(f"  {export_df.shape[0]:,} rows, {size_mb:.1f} MB")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Eagle quick tick exploration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("symbol", choices=["GC", "NQ"], help="Symbol to explore")
    parser.add_argument("--contract", "-c", help="Filter a contract (e.g. GCJ26-COMEX)")
    parser.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD")
    parser.add_argument("--hours", help="UTC hour range (e.g. 13-22 for RTH)")
    parser.add_argument("--export", help="Export to CSV: 'all' or 'topN' (e.g. top500)")
    parser.add_argument("--big", type=int, default=20, help="Number of large trades to show (default: 20)")
    parser.add_argument("--days", type=int, default=20, help="Number of recent days to show (default: 20)")
    parser.add_argument("--last", type=int, default=30, help="Last N days (default: 30)")
    parser.add_argument("--all", dest="all_data", action="store_true", help="Full history (ignores --last)")
    parser.add_argument("--no-hourly", action="store_true", help="Skip the hourly profile")

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  {BOLD}EAGLE — Tick Explorer [{args.symbol}]{RESET}")
    print(f"{'='*60}")

    # Load
    df = load_ticks(args.symbol)

    # Filter
    df = apply_filters(df, args)

    if df.shape[0] == 0:
        print(f"\n  No tick after filtering.")
        sys.exit(0)

    # Display
    print_overview(df, args.symbol)
    print_contract_stats(df)
    print_daily_stats(df, limit=args.days)
    print_big_trades(df, top_n=args.big)

    if not args.no_hourly:
        print_hourly_profile(df)

    # Export if requested
    if args.export:
        export_ticks(df, args.symbol, args.export, args)

    print(f"\n{'='*60}")
    print()


if __name__ == "__main__":
    main()
