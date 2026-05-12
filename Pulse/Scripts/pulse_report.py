#!/usr/bin/env python3
"""
pulse_report.py — Pulse trading terminal report

Usage:
    python3 Scripts/pulse_report.py
    python3 Scripts/pulse_report.py --symbol GC

Reads Live_Data + contracts.json + Intel_Data to display:
- Live price and rollover status
- Composite score (session + flow + verdict)
- Recent signals with edge
"""

import json
import sys
from pathlib import Path

try:
    import polars as pl
except ImportError:
    print("pip3 install polars")
    sys.exit(1)


# ═════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═════════════════════════════════════════════════════════════════════════════

BASE_DIR  = Path(__file__).parent.parent
DATA_DIR  = BASE_DIR / "Data"
LIVE_DIR  = DATA_DIR / "Live_Data"
FLUX_DIR  = DATA_DIR / "Flux_Data"
INTEL_DIR = DATA_DIR / "Intel_Data"
SYMBOLS   = ["GC", "NQ"]

NAMES = {"GC": "Gold COMEX", "NQ": "Nasdaq 100"}

# ANSI colors
G  = "\033[92m"   # green
R  = "\033[91m"   # red
Y  = "\033[93m"   # yellow
C  = "\033[96m"   # cyan
W  = "\033[97m"   # white
D  = "\033[90m"   # dim
B  = "\033[1m"    # bold
X  = "\033[0m"    # reset

# Import composite scoring from pulse_institutional
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from pulse_institutional import (
        compute_composite, SIGNAL_EDGE,
        SESSION_SIGNALS, FLOW_SIGNALS, FLOW_WINDOWS,
    )
    HAS_COMPOSITE = True
except ImportError:
    HAS_COMPOSITE = False


# ═════════════════════════════════════════════════════════════════════════════
# DATA
# ═════════════════════════════════════════════════════════════════════════════

def load_contracts() -> dict:
    p = DATA_DIR / "contracts.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {}


def load_live(sym):
    p = LIVE_DIR / f"{sym}_Live.csv"
    if not p.exists() or p.stat().st_size == 0:
        return None
    try:
        df = pl.read_csv(p, try_parse_dates=True)
        if df.is_empty():
            return None
        df = df.with_columns(pl.col("datetime_utc").cast(pl.Datetime("us")))
        return df
    except Exception:
        return None


def load_intel(sym):
    """Load institutional signals."""
    p = INTEL_DIR / f"{sym}_Institutional.parquet"
    if not p.exists():
        return None
    try:
        return pl.read_parquet(p)
    except Exception:
        return None


def load_volume_profile(sym):
    """Load the Volume Profile."""
    p = INTEL_DIR / f"{sym}_VolumeProfile.parquet"
    if not p.exists():
        return None
    try:
        return pl.read_parquet(p)
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# COMPOSITE DISPLAY
# ═════════════════════════════════════════════════════════════════════════════

def print_composite_report(sym, signals):
    """Print the composite score in the report."""
    if not HAS_COMPOSITE or signals is None:
        return

    results = compute_composite(signals, sym)
    if not results:
        return

    session_bias = results.get("session_bias", 0.0)
    session_details = results.get("session_details", [])
    windows = results.get("windows", {})

    # ── Session ──
    if session_details:
        s_color = G if session_bias > 0 else R if session_bias < 0 else ""
        s_rst = X if s_color else ""
        s_arrow = "▲" if session_bias > 0 else "▼" if session_bias < 0 else "●"
        print(f"     {B}Session{X} {s_color}{s_arrow}{s_rst} ", end="")
        print(f"{D}{', '.join(session_details)}{X}")
    else:
        print(f"     {B}Session{X} {D}● no signal{X}")

    # ── Flow (30m) ──
    w30 = windows.get(30, {})
    if w30:
        fs = w30["flow_score"]
        nf = w30["n_flow"]
        nt = w30["n_total"]
        if nf > 0:
            f_color = G if fs > 0 else R if fs < 0 else ""
            f_rst = X if f_color else ""
            f_arrow = "▲" if fs > 0 else "▼" if fs < 0 else "●"
            print(f"     {B}Flow 30m{X} {f_color}{f_arrow} {fs:+.2f}{f_rst}  "
                  f"{D}({nf} signals with edge / {nt} total){X}")
        else:
            print(f"     {B}Flow 30m{X} {D}● {nt} signals, none with edge{X}")

    # ── Verdict ──
    if w30:
        d = w30["direction"]
        c = w30["conviction"]
        has_session = abs(session_bias) > 0.01
        has_flow = w30["n_flow"] >= 1

        if (has_session or has_flow) and abs(d) >= 0.05:
            v_color = G if d > 0 else R
            v_rst = X
            v_arrow = "▲" if d > 0 else "▼"
            v_word = "BULLISH" if d > 0 else "BEARISH"
            if abs(d) >= 0.50 and c >= 0.80:
                strength = "STRONG"
            elif abs(d) >= 0.20 and c >= 0.60:
                strength = "MODERATE"
            else:
                strength = "WEAK"
            sources = []
            if has_session:
                sources.append("session")
            if has_flow:
                sources.append("flow")
            print(f"     {v_color}{B}{v_arrow} {v_word} {strength}{v_rst}"
                  f"  {D}dir={d:+.2f} conv={c:.0%} ({' + '.join(sources)}){X}")
        else:
            print(f"     {Y}● NEUTRAL{X}  {D}dir={d:+.2f}{X}")


def print_recent_signals(sym, signals, n=5):
    """Print the N most recent signals with edge."""
    if signals is None:
        return

    edges = SIGNAL_EDGE.get(sym, {})

    # Signals with edge > 0.01, sorted by date descending
    recent = signals.sort("datetime_utc", descending=True)

    printed = 0
    seen = set()
    for row in recent.iter_rows(named=True):
        sig = row["signal"]
        edge = edges.get(sig, 0.0)
        if edge < 0.01:
            continue

        ts = row["datetime_utc"]
        ts_str = ts.strftime("%H:%M:%S") if hasattr(ts, 'strftime') else str(ts)[-8:]
        details = row.get("details", "") or ""
        score = row.get("score", 0.0) or 0.0

        # Avoid duplicates of the same type within the same minute
        key = f"{sig}_{ts_str[:5]}"
        if key in seen:
            continue
        seen.add(key)

        color = G if "★★★" in _stars(edge) else Y if "★★" in _stars(edge) else ""
        rst = X if color else ""
        print(f"     {D}{ts_str}{X}  {color}{sig:20s}{rst}  "
              f"{D}{details[:40]}{X}")

        printed += 1
        if printed >= n:
            break


def _stars(edge):
    if edge >= 0.70:
        return "★★★★"
    if edge >= 0.10:
        return "★★★"
    if edge >= 0.05:
        return "★★"
    return "★"


# ═════════════════════════════════════════════════════════════════════════════
# REPORT
# ═════════════════════════════════════════════════════════════════════════════

def print_report(sym, contracts):
    df = load_live(sym)
    name = NAMES[sym]
    info = contracts.get(sym, {})

    front = info.get("front", "?")
    rollover = info.get("rollover", {})

    # Live price
    if df is not None:
        last_row   = df.sort("datetime_utc").row(-1, named=True)
        last_price = last_row["close"]
        last_time  = last_row["datetime_utc"]
        n_ticks    = df.shape[0]
        print(f"  {C}{name} ({sym}){X}  {W}[{front}]{X}")
        print(f"     {W}{B}{last_price:,.2f}{X}  {C}@ {last_time.strftime('%H:%M:%S')} UTC{X}  {D}({n_ticks:,} ticks){X}")
    else:
        print(f"  {C}{name} ({sym}){X}  {W}[{front}]{X}")
        print(f"     {D}no live data{X}")

    # Rollover
    if rollover:
        status = rollover.get("status", "?")
        ratio  = rollover.get("ratio", 0)
        fvol   = rollover.get("front_vol", 0)
        nvol   = rollover.get("next_vol", 0)
        next_c = info.get("next", "?")
        print(f"     {D}rollover: {status}  {front}:{fvol:,} vs {next_c}:{nvol:,} ({ratio:.0%}){X}")

    # Volume Profile
    vp = load_volume_profile(sym)
    if vp is not None and vp.shape[0] >= 1:
        last_vp = vp.sort("date").row(-1, named=True)
        poc = last_vp["poc"]
        vah = last_vp["vah"]
        val = last_vp["val"]
        mig = last_vp["migration"]
        vp_date = last_vp["date"]

        # Price position relative to the VA
        if df is not None:
            last_price = df.sort("datetime_utc").row(-1, named=True)["close"]
            if last_price > vah:
                pos = f"{G}above VA{X}"
            elif last_price < val:
                pos = f"{R}below VA{X}"
            else:
                pos = f"{Y}inside VA{X}"
        else:
            pos = ""

        mig_colors = {
            "HIGHER_VALUE": G, "HIGHER_OVERLAP": G,
            "LOWER_VALUE": R, "LOWER_OVERLAP": R,
            "INSIDE": Y, "OUTSIDE": C,
        }
        mc = mig_colors.get(mig, D)

        print(f"     {B}VP{X} {D}({vp_date}){X}  "
              f"POC {W}{B}{poc:,.2f}{X}  "
              f"VA [{val:,.2f} — {vah:,.2f}]  "
              f"{mc}{mig}{X}  {pos}")

    # Composite + recent signals
    intel = load_intel(sym)
    if intel is not None:
        print_composite_report(sym, intel)
        print_recent_signals(sym, intel)
    else:
        print(f"     {D}(run pulse-inst for the composite score){X}")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    args = sys.argv[1:]
    sym_filter = None

    for i, a in enumerate(args):
        if a == "--symbol" and i + 1 < len(args):
            sym_filter = args[i + 1].upper()

    symbols = [sym_filter] if sym_filter else SYMBOLS
    contracts = load_contracts()

    print()
    print(f"  {B}{C}  PULSE{X}")
    print()

    for sym in symbols:
        print_report(sym, contracts)
        print()

    print()


if __name__ == "__main__":
    main()
