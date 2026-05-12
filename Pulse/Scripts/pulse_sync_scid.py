#!/usr/bin/env python3
"""
pulse_sync_scid.py — Copy .scid files (Last, Front, Next) from Sierra Chart

Usage:
    python3 Scripts/pulse_sync_scid.py

Source : /Volumes/[C] Windows 11/SierraChart/Data
Dest   : /Users/m8raven/Documents/Projets/TCoding/Pulse/Data/Scid_Data/

Behavior:
    - Determines Last, Front and Next based on the current month
    - Copies only if the source .scid is larger or more recent
    - Cleans up old .scid files that are no longer Last/Front/Next
    - Writes contracts.json (pulse_bridge.py reads it to filter the Front)
"""

import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

SC_LIVE_DIR = Path("/Volumes/[C] Windows 11/SierraChart/Data")
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "Data"
SCID_DIR    = DATA_DIR / "Scid_Data"

# CME futures month codes
MONTH_CODES = {1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
               7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"}

# GC = bimonthly (G, J, M, Q, V, Z)
#      rollover ~26th of the month preceding delivery (e.g., GCJ26 Apr → roll late March)
# NQ = quarterly (H, M, U, Z)
#      rollover ~12th of the expiration month (e.g., NQH26 March → roll ~March 12)
CONTRACTS = {
    "GC": {"exchange": "COMEX", "months": [2, 4, 6, 8, 10, 12], "rollover_day": 26},
    "NQ": {"exchange": "CME",   "months": [3, 6, 9, 12],        "rollover_day": 12},
}

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# CONTRACT RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_contracts(symbol: str, now: datetime) -> dict:
    """
    Determine Last, Front and Next for a symbol.
    Returns {"last": "GCG26", "front": "GCJ26", "next": "GCM26"}

    Logic: generate all calendar contracts within a ±2-year window,
    compute their rollover date, and take the first contract whose rollover
    is in the future. That contract is the Front (it has not yet rolled).

    Rollover dates:
        NQ: ~8 days before the 3rd Friday of the expiration month → day 12 of expiration month
        GC: ~3 business days before the 1st notice day → day 26 of month preceding delivery

    April 2026 fix: the old algorithm did not detect a rollover when in a later month
    (e.g., April, GCJ26 had rolled in March but remained Front).
    New algorithm: rollover-date-based, robust to silent weeks.
    """
    cfg = CONTRACTS[symbol]
    exchange = cfg["exchange"]
    cycle = cfg["months"]
    rollover_day = cfg.get("rollover_day", 12)
    base_year = now.year

    # Generate all (year, contract-month) pairs in the window
    candidates = []
    for y in range(base_year - 2, base_year + 3):
        for m in cycle:
            # Rollover date for THIS contract
            if symbol == "GC":
                # GC: rollover in the month BEFORE delivery
                if m == 1:
                    roll_y, roll_m = y - 1, 12
                else:
                    roll_y, roll_m = y, m - 1
            else:
                # NQ: rollover in the expiration month
                roll_y, roll_m = y, m
            roll_date = datetime(roll_y, roll_m, rollover_day)
            candidates.append((y, m, roll_date))

    # Sort by rollover date
    candidates.sort(key=lambda c: c[2])

    # The Front is the first contract whose rollover is in the future
    front_idx = None
    for i, c in enumerate(candidates):
        if c[2] > now:
            front_idx = i
            break

    if front_idx is None:
        raise RuntimeError(f"All {symbol} contracts are in the past — extend CONTRACTS[{symbol}]")

    last_cand  = candidates[front_idx - 1] if front_idx > 0 else candidates[0]
    front_cand = candidates[front_idx]
    next_cand  = candidates[front_idx + 1] if front_idx + 1 < len(candidates) else front_cand

    def fmt(cand):
        y, m, _ = cand
        name = f"{symbol}{MONTH_CODES[m]}{y % 100:02d}"
        return {"contract": name, "file": f"{name}-{exchange}.scid"}

    return {
        "last":  fmt(last_cand),
        "front": fmt(front_cand),
        "next":  fmt(next_cand),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    now = datetime.now()

    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {BOLD}{CYAN}  PULSE — Sync Scid (Last / Front / Next){RESET}")
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  Source : {SC_LIVE_DIR}")
    print(f"  Dest   : {SCID_DIR}")
    print(f"  Date   : {now.strftime('%Y-%m-%d')}")
    print()

    # Check that the Sierra Chart volume is mounted
    if not SC_LIVE_DIR.exists():
        print(f"  {RED}Sierra Chart volume not mounted.{RESET}")
        print(f"  {YELLOW}Start Parallels + Windows and rerun.{RESET}")
        sys.exit(1)

    SCID_DIR.mkdir(parents=True, exist_ok=True)

    # Resolve contracts
    contracts_json = {}
    needed_files = set()

    for symbol in CONTRACTS:
        resolved = resolve_contracts(symbol, now)
        contracts_json[symbol] = {
            "last":  resolved["last"]["contract"],
            "front": resolved["front"]["contract"],
            "next":  resolved["next"]["contract"],
        }

        print(f"  {BOLD}{symbol}{RESET}")
        for role in ["last", "front", "next"]:
            label = {"last": "Last ", "front": "Front", "next": "Next "}.get(role)
            print(f"     {label} : {resolved[role]['contract']}")
            needed_files.add(resolved[role]["file"])
        print()

    # Sync
    copied   = 0
    skipped  = 0
    missing  = 0
    total_mb = 0.0

    for filename in sorted(needed_files):
        src = SC_LIVE_DIR / filename
        dst = SCID_DIR / filename

        if not src.exists():
            print(f"  {YELLOW}[SKIP]{RESET} {filename} — not found (not yet active)")
            missing += 1
            continue

        src_size = src.stat().st_size

        if dst.exists() and dst.stat().st_size == src_size:
            print(f"  {CYAN}[OK]{RESET}   {filename} — up to date")
            skipped += 1
            continue

        size_mb = src_size / 1e6
        print(f"  {GREEN}[COPY]{RESET} {filename} ({size_mb:.0f} MB)...", end=" ", flush=True)
        shutil.copy2(src, dst)
        print("✓")
        copied   += 1
        total_mb += size_mb

    # Clean up old .scid files
    existing = set(f.name for f in SCID_DIR.glob("*.scid"))
    obsolete = existing - needed_files
    if obsolete:
        print()
        for old in sorted(obsolete):
            old_path = SCID_DIR / old
            size_mb = old_path.stat().st_size / 1e6
            print(f"  {YELLOW}[CLEAN]{RESET} {old} ({size_mb:.0f} MB) — no longer needed")
            old_path.unlink()

    # Write contracts.json
    json_path = DATA_DIR / "contracts.json"
    with open(json_path, "w") as f:
        json.dump(contracts_json, f, indent=2)
    print(f"  {GREEN}[JSON]{RESET} contracts.json ✓")

    # Summary
    print()
    print(f"  {'─' * 45}")
    print(f"  Copied  : {copied} files ({total_mb:.0f} MB)")
    print(f"  Up to date: {skipped} files")
    if missing:
        print(f"  {YELLOW}Missing : {missing} files (not yet active){RESET}")
    if obsolete:
        print(f"  {YELLOW}Cleaned : {len(obsolete)} old files{RESET}")
    print()


if __name__ == "__main__":
    main()
