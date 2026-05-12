#!/usr/bin/env python3
"""
pulse_listen.py — Listen to Sierra Chart UDP feed → accumulate in CSV

Usage:
    python3 pulse_listen.py

Produces:
    /Users/m8raven/Documents/Projets/TCoding/Pulse/Data/Live_Data/GC_Live.csv
    /Users/m8raven/Documents/Projets/TCoding/Pulse/Data/Live_Data/NQ_Live.csv

Behavior:
    - Writes to *_Live.csv (NEVER to *_Tick_Flux.parquet)
    - Each tick is written immediately (append, no buffer)
    - pulse_bridge.py merges Live + Scid → Tick_Flux
    - Deduplication by (timestamp + price)
    - Startup log: how many ticks already on disk
"""

import csv
import socket
import sys
from datetime import datetime
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR   = Path(__file__).parent.parent
DATA_DIR   = BASE_DIR / "Data"
LIVE_DIR   = DATA_DIR / "Live_Data"
LIVE_DIR.mkdir(parents=True, exist_ok=True)

UDP_HOST   = "0.0.0.0"
UDP_PORT   = 11099

SYMBOLS    = {"GC", "NQ"}

CSV_COLS = ["datetime_utc", "contract", "open", "high", "low",
            "close", "volume", "bid_vol", "ask_vol", "delta"]

# Terminal colors
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# CSV HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def csv_path(symbol: str) -> Path:
    return LIVE_DIR / f"{symbol}_Live.csv"


def ensure_header(symbol: str):
    """Create the CSV file with header if it doesn't exist."""
    p = csv_path(symbol)
    if not p.exists() or p.stat().st_size == 0:
        with open(p, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_COLS)


def count_lines(symbol: str) -> int:
    """Count data lines (excluding header)."""
    p = csv_path(symbol)
    if not p.exists():
        return 0
    with open(p) as f:
        return max(0, sum(1 for _ in f) - 1)


def append_tick(symbol: str, row: list):
    """Append a row to the CSV — immediate flush."""
    with open(csv_path(symbol), "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def print_status(symbol: str):
    """Print CSV status at startup."""
    n = count_lines(symbol)
    if n == 0:
        print(f"  {CYAN}[{symbol}]{RESET} New — no ticks")
        return
    size = csv_path(symbol).stat().st_size / 1e6
    print(f"  {CYAN}[{symbol}]{RESET} {n:,} ticks — {size:.2f} MB")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print()
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  {BOLD}{CYAN}  PULSE — Live Tick Listener{RESET}")
    print(f"  {BOLD}{'═' * 55}{RESET}")
    print(f"  UDP  : {UDP_HOST}:{UDP_PORT}")
    print(f"  Data : {LIVE_DIR}")
    print()

    # Prepare CSVs
    for sym in SYMBOLS:
        ensure_header(sym)
        print_status(sym)

    print()

    tick_totals : dict[str, int] = {s: 0 for s in SYMBOLS}
    last_tick   : dict[str, str] = {}   # dedup

    # UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, UDP_PORT))
    sock.settimeout(1.0)

    print(f"  {GREEN}Listening — Ctrl+C to stop{RESET}")
    print()

    try:
        while True:
            try:
                data, _ = sock.recvfrom(1024)
                msg = data.decode("utf-8").strip()
            except socket.timeout:
                continue
            except Exception:
                continue

            # Sierra Chart format: symbol,timestamp,price,volume,bid_vol,ask_vol
            parts = msg.split(",")
            if len(parts) != 6:
                continue

            symbol_raw, ts_str, price_s, vol_s, bid_s, ask_s = parts

            # Extract symbol (e.g., "NQM26-CME" → "NQ")
            sym      = symbol_raw.split("-")[0][:2].upper()
            contract = symbol_raw.split("-")[0].upper()   # e.g., NQM26
            if sym not in SYMBOLS:
                continue

            # Deduplication
            tick_id = ts_str + price_s
            if tick_id == last_tick.get(sym):
                continue
            last_tick[sym] = tick_id

            # Parse values
            try:
                price   = float(price_s)
                volume  = int(float(vol_s))
                bid_vol = int(float(bid_s))
                ask_vol = int(float(ask_s))
            except ValueError:
                continue

            # Parse timestamp
            try:
                dt = datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                try:
                    dt = datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

            delta = ask_vol - bid_vol

            # Write immediately to disk
            row = [dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
                   contract, price, price, price, price,
                   volume, bid_vol, ask_vol, delta]
            append_tick(sym, row)

            tick_totals[sym] += 1

            # Log first ticks + every 100th
            n = tick_totals[sym]
            if n <= 3 or n % 100 == 0:
                print(
                    f"  {CYAN}[{contract}]{RESET} #{n:,} "
                    f"{price_s} vol={volume} Δ={delta:+d} [{ts_str}]"
                )

    except KeyboardInterrupt:
        print(f"\n  {YELLOW}Stopped{RESET}")

    finally:
        sock.close()
        print()
        for sym in SYMBOLS:
            n = tick_totals[sym]
            if n > 0:
                print(f"  {GREEN}[{sym}] {n:,} ticks added{RESET}")
        print(f"\n  {BOLD}Pulse stopped cleanly.{RESET}\n")


if __name__ == "__main__":
    main()
