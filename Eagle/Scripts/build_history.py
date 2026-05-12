#!/usr/bin/env python3
"""
build_history.py — Convert .scid into tick-level Parquet for ML
Runs on MAC — Reads Sierra Chart live (active) + TC_Sam128 (archive),
              exports to /Volumes/Sam128/TC_Sam128/Ticks_Parquet/

Usage:
    pip3 install polars
    cd ~/Documents/Projets/TCoding/Eagle
    python3 Scripts/build_history.py           # smart: front + next only
    python3 Scripts/build_history.py --full    # full rebuild (all contracts)

Default mode (smart):
    Rebuild only if the last 2 contracts (front + next) have changed.
    Historical contracts never change — no need to re-read them.

--full mode:
    Rebuild everything from scratch. Use if a historical contract was
    corrected, or for the initial first build.

Path B (2026-05-12):
    .scid lookup order is Sierra > Pulse/Data/Scid_Data > TC_Sam128.
    Pulse owns .scid syncing; Eagle no longer copies from Sierra.
    Parquet output lives on Sam128 (TC_Sam128/Ticks_Parquet/).
"""
import struct
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("Install polars: pip3 install polars")
    exit(1)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent.parent          # Eagle/
SC_LIVE_DIR = Path("/Volumes/[C] Windows 11/SierraChart/Data")
PULSE_SCID_DIR = BASE_DIR.parent / "Pulse" / "Data" / "Scid_Data"
TC_ARCHIVE_DIR = Path("/Volumes/Sam128/TC_Sam128")
OUTPUT_DIR = TC_ARCHIVE_DIR / "Ticks_Parquet"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Contract chains — oldest to most recent (front month)
CONTRACT_CHAINS = {
    "GC": [
        "GCG24-COMEX.scid",   # Aug 2023 → Feb 2024
        "GCQ24-COMEX.scid",   # Feb 2024 → Aug 2024
        "GCZ24-COMEX.scid",   # Jun 2024 → Dec 2024
        "GCG25-COMEX.scid",   # Aug 2024 → Feb 2025
        "GCJ25-COMEX.scid",   # Oct 2024 → Apr 2025
        "GCM25-COMEX.scid",   # Dec 2024 → Jun 2025
        "GCQ25-COMEX.scid",   # Feb 2025 → Aug 2025
        "GCG26-COMEX.scid",   # Aug 2025 → Feb 2026
        "GCJ26-COMEX.scid",   # Sep 2025 → Mar 2026
        "GCM26-COMEX.scid",   # Dec 2025 → Jun 2026 (next front)
    ],
    "NQ": [
        "NQH24-CME.scid",     # Sep 2023 → Mar 2024
        "NQM24-CME.scid",     # Dec 2023 → Jun 2024
        "NQU24-CME.scid",     # Mar 2024 → Aug 2024
        "NQZ24-CME.scid",     # Jun 2024 → Dec 2024
        "NQH25-CME.scid",     # Sep 2024 → Mar 2025
        "NQM25-CME.scid",     # Dec 2024 → Jun 2025
        "NQU25-CME.scid",     # Mar 2025 → Sep 2025
        "NQZ25-CME.scid",     # Jun 2025 → Dec 2025
        "NQH26-CME.scid",     # Sep 2025 → Mar 2026
        "NQM26-CME.scid",     # Dec 2025 → now (front month)
    ],
}

# SCID
HEADER_SIZE = 56
RECORD_SIZE = 40
RECORD_FMT = "<qffffIIII"
EXCEL_EPOCH = datetime(1899, 12, 30)
CHUNK_SIZE = 500_000


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Note: sync to a local cache used to live here (sync_raw_scid).
# Path B (2026-05-12): Pulse owns .scid syncing via `pulse_sync_scid.py`.
# Eagle now reads through Sierra → Pulse/Data/Scid_Data/ → TC_Sam128 directly.


def find_scid(filename):
    """Search for a .scid: Sierra Chart live > Pulse working copy > TC archive.

    Sierra holds the active 6 contracts (last/front/next per symbol) live.
    Pulse keeps a working copy of those same 6 (refreshed by Pulse `sync`).
    TC_Sam128 holds all 20 (active + 16 historical).
    """
    for d in [SC_LIVE_DIR, PULSE_SCID_DIR, TC_ARCHIVE_DIR]:
        p = d / filename
        if p.exists():
            return p
    return None


def read_scid_to_lists(filepath):
    """Read a .scid and return lists for Polars (memory-efficient)."""
    fsize = os.path.getsize(filepath)
    nrec = (fsize - HEADER_SIZE) // RECORD_SIZE

    timestamps = []
    opens = []
    highs = []
    lows = []
    closes = []
    num_trades = []
    volumes = []
    bid_vols = []
    ask_vols = []

    with open(filepath, "rb") as f:
        f.seek(HEADER_SIZE)
        for start in range(0, nrec, CHUNK_SIZE):
            n = min(CHUNK_SIZE, nrec - start)
            data = f.read(n * RECORD_SIZE)
            for rec in struct.iter_unpack(RECORD_FMT, data):
                # Convert SCDateTimeMS to datetime
                dt = EXCEL_EPOCH + timedelta(microseconds=rec[0])
                timestamps.append(dt)
                opens.append(round(rec[1], 2))
                highs.append(round(rec[2], 2))
                lows.append(round(rec[3], 2))
                closes.append(round(rec[4], 2))
                num_trades.append(rec[5])
                volumes.append(rec[6])
                bid_vols.append(rec[7])
                ask_vols.append(rec[8])

    return {
        "datetime_utc": timestamps,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "num_trades": num_trades,
        "volume": volumes,
        "bid_vol": bid_vols,
        "ask_vol": ask_vols,
    }


def scid_tick_count(filepath):
    """Number of ticks in a .scid (computed from file size)."""
    return (os.path.getsize(filepath) - HEADER_SIZE) // RECORD_SIZE


def read_meta(symbol):
    """Read the meta file (tick counts per contract)."""
    meta_path = OUTPUT_DIR / f"{symbol}_meta.json"
    if meta_path.exists():
        with open(meta_path) as f:
            return json.load(f)
    return {}


def write_meta(symbol, chain):
    """Write the meta file with the current tick counts."""
    meta = {}
    for filename in chain:
        filepath = find_scid(filename)
        if filepath:
            meta[filename.replace(".scid", "")] = scid_tick_count(filepath)
    meta_path = OUTPUT_DIR / f"{symbol}_meta.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)


def needs_rebuild(symbol, chain, full=False):
    """Check whether the parquet must be rebuilt.

    Compares .scid tick counts vs the last build (meta JSON).
    More reliable than mtime — only rebuilds if there are actually new ticks.
    """
    out_path = OUTPUT_DIR / f"{symbol}_ticks.parquet"
    if not out_path.exists():
        return True
    if full:
        return True

    # Smart: compare tick counts of the 2 active contracts
    meta = read_meta(symbol)
    for filename in chain[-2:]:
        contract = filename.replace(".scid", "")
        filepath = find_scid(filename)
        if not filepath:
            continue
        current = scid_tick_count(filepath)
        previous = meta.get(contract, -1)
        if current != previous:
            print(f"  {contract}: {previous:,} → {current:,} ticks (+{current - max(previous, 0):,})")
            return True

    return False


def read_contracts(filenames):
    """Read a list of .scid and return a Polars DataFrame."""
    frames = []
    for filename in filenames:
        filepath = find_scid(filename)
        if not filepath:
            print(f"  [{filename}] not found — skip")
            continue

        fsize = os.path.getsize(filepath)
        nrec = (fsize - HEADER_SIZE) // RECORD_SIZE
        print(f"  [{filename}] {nrec:,} ticks ({fsize/1e6:.0f} MB)...", end=" ", flush=True)

        data = read_scid_to_lists(filepath)
        contract_name = filename.replace(".scid", "")

        df = pl.DataFrame(data).with_columns([
            pl.lit(contract_name).alias("contract"),
            (pl.col("ask_vol").cast(pl.Int64) - pl.col("bid_vol").cast(pl.Int64)).alias("delta"),
        ])
        frames.append(df)
        print(f"OK ({df.shape[0]:,} rows)")

    if not frames:
        return None
    return pl.concat(frames)


def build_full(symbol, chain):
    """Rebuild the full Parquet from all contracts."""
    print(f"\n{'─'*60}")
    print(f"  {symbol} — FULL build ({len(chain)} contracts)")
    print(f"{'─'*60}")

    combined = read_contracts(chain)
    if combined is None:
        print(f"  ERROR: no data for {symbol}")
        return None

    result = save_parquet(symbol, combined)
    write_meta(symbol, chain)
    return result


def build_smart(symbol, chain):
    """Rebuild keeping existing history, re-read only the last 2 contracts."""
    out_path = OUTPUT_DIR / f"{symbol}_ticks.parquet"
    active_contracts = chain[-2:]
    active_names = [f.replace(".scid", "") for f in active_contracts]

    print(f"\n{'─'*60}")
    print(f"  {symbol} — SMART build (updating: {', '.join(active_names)})")
    print(f"{'─'*60}")

    # Load existing parquet, drop contracts to be replaced
    print(f"  Loading existing parquet...", end=" ", flush=True)
    existing = pl.read_parquet(out_path)
    n_before = existing.shape[0]
    historical = existing.filter(~pl.col("contract").is_in(active_names))
    n_kept = historical.shape[0]
    print(f"{n_before:,} ticks, keeping {n_kept:,} historical")

    # Read active contracts from fresh .scid
    fresh = read_contracts(active_contracts)
    if fresh is None:
        print(f"  No active contract found — keeping existing")
        return existing

    # Sort only the fresh ones, history is already sorted
    fresh = fresh.sort("datetime_utc")

    # Combine history + fresh (no global re-sort — too heavy on 268M rows)
    combined = pl.concat([historical, fresh])

    result = save_parquet(symbol, combined, skip_sort=True)
    write_meta(symbol, chain)
    return result


def save_parquet(symbol, combined, skip_sort=False):
    """Sort (optional), add delta, save the Parquet."""
    if skip_sort:
        print(f"\n  Merging (skip sort — history already sorted)...")
    else:
        print(f"\n  Merging and sorting by date...")
        combined = combined.sort("datetime_utc")

    # Add delta (in case it's missing)
    if "delta" not in combined.columns:
        combined = combined.with_columns(
            (pl.col("ask_vol").cast(pl.Int64) - pl.col("bid_vol").cast(pl.Int64)).alias("delta")
        )

    # Stats
    first = combined["datetime_utc"][0]
    last = combined["datetime_utc"][-1]
    days = (last - first).days
    contracts = combined["contract"].n_unique()

    print(f"  Total: {combined.shape[0]:,} ticks, {contracts} contracts")
    print(f"  Range: {first.strftime('%Y-%m-%d')} → {last.strftime('%Y-%m-%d')} ({days} days)")

    # Export Parquet
    out_path = OUTPUT_DIR / f"{symbol}_ticks.parquet"
    combined.write_parquet(out_path, compression="zstd")
    print(f"  → {out_path}")
    print(f"  Size: {os.path.getsize(out_path)/1e6:.1f} MB")

    return combined


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    full_mode = "--full" in sys.argv
    mode_label = "FULL" if full_mode else "SMART"

    print("=" * 60)
    print(f"  EAGLE — Build History [{mode_label}]")
    print("=" * 60)
    print(f"  Sierra   : {SC_LIVE_DIR}")
    print(f"  Pulse    : {PULSE_SCID_DIR}")
    print(f"  Archive  : {TC_ARCHIVE_DIR}")
    print(f"  Output   : {OUTPUT_DIR}")
    if not full_mode:
        print(f"  Mode     : Smart (only the 2 latest contracts)")
        print(f"             Use --full to rebuild everything")

    rebuilt = 0
    for symbol, chain in CONTRACT_CHAINS.items():
        if needs_rebuild(symbol, chain, full=full_mode):
            if full_mode or not (OUTPUT_DIR / f"{symbol}_ticks.parquet").exists():
                build_full(symbol, chain)
            else:
                build_smart(symbol, chain)
            rebuilt += 1
        else:
            print(f"\n  {symbol} — parquet up to date, skip")

    print(f"\n{'='*60}")
    if rebuilt:
        print(f"  Done. {rebuilt} symbol(s) rebuilt. [{mode_label}]")
    else:
        print("  All up to date — nothing to rebuild.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
