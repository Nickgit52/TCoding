#!/usr/bin/env python3
"""
daily_briefing.py — Eagle daily briefing
Runs on MAC — Generates a text summary with levels, context, and signals.

Usage:
    cd ~/Documents/Projets/TCoding/Eagle
    python3 Scripts/daily_briefing.py              # NQ (default)
    python3 Scripts/daily_briefing.py GC            # GC
    python3 Scripts/daily_briefing.py NQ --price 24800   # force the reference price
    python3 Scripts/daily_briefing.py NQ --save     # save to Reports/

The briefing is designed to be copied into an AI (Claude, etc.) as pre-analyzed
context to confirm or guide a trading decision.

Targets:
    T1 : ~20-25 pts (quick scalp)
    T2 : ~50 pts (main target)
    T3 : trailing (runner, trend days)
"""
import sys
import os
import argparse
from datetime import datetime, timedelta, date
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
CANDLES_DIR = BASE_DIR / "Data" / "Candles"
PROFILE_CSV = BASE_DIR / "Data" / "Reports" / "Market_Profile" / "daily_profile.csv"
NAKED_CSV = BASE_DIR / "Data" / "Reports" / "Market_Profile" / "naked_poc.csv"
REGIMES_CSV = BASE_DIR / "Data" / "Reports" / "Order_Flow" / "orderflow_regimes.csv"
REPORTS_DIR = BASE_DIR / "Data" / "Reports" / "Briefings"

# Targets in points
TARGETS = {
    "NQ": {"T1": 25, "T2": 50, "T3": 100},
    "GC": {"T1": 5, "T2": 12, "T3": 25},
}

# IB hours UTC
IB_HOUR = {"GC": 13, "NQ": 14}

# RTH
RTH = {
    "GC": {"start": 13, "end": 22},
    "NQ": {"start": 13, "end": 21},
}

BOLD = "\033[1m"
CYAN = "\033[96m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_profile(symbol, n_days=10):
    """Load the last N days of the market profile."""
    if not PROFILE_CSV.exists():
        return None
    df = pl.read_csv(PROFILE_CSV)
    df = df.filter(pl.col("symbol") == symbol).sort("date", descending=True).head(n_days)
    return df


def load_full_profile(symbol):
    """Load the full market profile for weekly computations."""
    if not PROFILE_CSV.exists():
        return None
    df = pl.read_csv(PROFILE_CSV)
    df = df.filter(pl.col("symbol") == symbol).sort("date")
    return df


def load_naked_pocs(symbol, price_ref, radius=500):
    """Load naked POCs close to the current price."""
    if not NAKED_CSV.exists():
        return []
    df = pl.read_csv(NAKED_CSV)
    naked = df.filter(
        (pl.col("symbol") == symbol) &
        (pl.col("filled") == False)
    ).sort("poc")

    # Filter by radius
    nearby = []
    for row in naked.iter_rows(named=True):
        poc = row["poc"]
        dist = poc - price_ref
        if abs(dist) <= radius:
            nearby.append({
                "date": row["origin_date"],
                "poc": poc,
                "type": row["day_type"],
                "age": row["days_alive"],
                "dist": dist,
            })

    # Sort by absolute distance
    nearby.sort(key=lambda x: abs(x["dist"]))
    return nearby


def load_recent_regimes(symbol, n_bars=36):
    """Load the last N regime bars (30 min = 6 × 5m bars)."""
    if not REGIMES_CSV.exists():
        return None
    df = pl.read_csv(REGIMES_CSV)
    df = df.filter(pl.col("symbol") == symbol).sort("datetime_utc", descending=True).head(n_bars)
    return df


def get_current_price(symbol):
    """Get the latest price and its timestamp from 5m candles."""
    path = CANDLES_DIR / f"{symbol}_5m.parquet"
    if not path.exists():
        return None, None
    df = pl.read_parquet(path)
    last = df.sort("datetime_utc").tail(1)
    if last.shape[0] == 0:
        return None, None
    return last["close"][0], last["datetime_utc"][0]


def get_today_candles(symbol):
    """Get today's 5m candles."""
    path = CANDLES_DIR / f"{symbol}_5m.parquet"
    if not path.exists():
        return None
    df = pl.read_parquet(path)
    today = datetime.utcnow().date()
    today_df = df.filter(pl.col("datetime_utc").dt.date() == today)
    return today_df


def compute_ib(candles_today, symbol):
    """Compute the Initial Balance from today's candles."""
    if candles_today is None or candles_today.shape[0] == 0:
        return None

    ib_hour = IB_HOUR.get(symbol, 14)
    ib_candles = candles_today.filter(pl.col("datetime_utc").dt.hour() == ib_hour)

    if ib_candles.shape[0] == 0:
        return None

    return {
        "high": ib_candles["high"].max(),
        "low": ib_candles["low"].min(),
        "range": ib_candles["high"].max() - ib_candles["low"].min(),
        "volume": ib_candles["volume"].sum(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def compute_trends(full_profile, price_ref, symbol):
    """Compute Weekly and Daily trends."""
    if full_profile is None or full_profile.shape[0] < 10:
        return None

    # --- DAILY TREND (last 5 days) ---
    daily = full_profile.tail(5)
    d_closes = daily["close" if "close" in daily.columns else "day_close"].to_list()
    d_opens = daily["day_open"].to_list()
    d_highs = daily["day_high"].to_list()
    d_lows = daily["day_low"].to_list()
    d_dates = daily["date"].to_list()

    # Direction: compare first open vs last close
    d_change = d_closes[-1] - d_opens[0]
    d_high = max(d_highs)
    d_low = min(d_lows)
    d_range = d_high - d_low

    # Higher highs / lower lows count
    hh = sum(1 for i in range(1, len(d_highs)) if d_highs[i] > d_highs[i - 1])
    ll = sum(1 for i in range(1, len(d_lows)) if d_lows[i] < d_lows[i - 1])

    # Strength (0-3): based on close consecutiveness
    up_days = sum(1 for i in range(1, len(d_closes)) if d_closes[i] > d_closes[i - 1])
    dn_days = sum(1 for i in range(1, len(d_closes)) if d_closes[i] < d_closes[i - 1])

    if d_change > 0:
        d_dir = "BULLISH"
        d_strength = min(3, up_days)
    elif d_change < 0:
        d_dir = "BEARISH"
        d_strength = min(3, dn_days)
    else:
        d_dir = "NEUTRAL"
        d_strength = 0

    # --- WEEKLY TREND (last 4 weeks) ---
    # Aggregate by ISO week
    df_w = full_profile.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("dt")
    ).with_columns(
        pl.col("dt").dt.iso_year().alias("iso_year"),
        pl.col("dt").dt.week().alias("iso_week"),
    )

    weeks = df_w.group_by(["iso_year", "iso_week"]).agg([
        pl.col("day_open").first().alias("w_open"),
        pl.col("day_high").max().alias("w_high"),
        pl.col("day_low").min().alias("w_low"),
        pl.col("day_close").last().alias("w_close"),
        pl.col("day_volume").sum().alias("w_vol"),
        pl.col("dt").min().alias("w_start"),
        pl.len().alias("w_days"),
    ]).sort("w_start")

    # Exclude current week if only 1 day (incomplete)
    last_week = weeks.tail(1)
    if last_week["w_days"][0] < 3:
        complete_weeks = weeks.head(weeks.shape[0] - 1)
    else:
        complete_weeks = weeks

    recent_4w = complete_weeks.tail(4)
    w_closes = recent_4w["w_close"].to_list()
    w_opens = recent_4w["w_open"].to_list()
    w_highs = recent_4w["w_high"].to_list()
    w_lows = recent_4w["w_low"].to_list()
    w_starts = recent_4w["w_start"].to_list()

    w_change = w_closes[-1] - w_opens[0]
    w_range = max(w_highs) - min(w_lows)

    w_up = sum(1 for i in range(len(w_closes)) if w_closes[i] > w_opens[i])
    w_dn = sum(1 for i in range(len(w_closes)) if w_closes[i] < w_opens[i])

    if w_change > 0:
        w_dir = "BULLISH"
        w_strength = min(3, w_up)
    elif w_change < 0:
        w_dir = "BEARISH"
        w_strength = min(3, w_dn)
    else:
        w_dir = "NEUTRAL"
        w_strength = 0

    # Visual strength
    def strength_bar(s):
        return "●" * s + "○" * (3 - s)

    return {
        "daily": {
            "direction": d_dir,
            "change": d_change,
            "strength": d_strength,
            "bar": strength_bar(d_strength),
            "range": d_range,
            "high": d_high,
            "low": d_low,
            "hh": hh,
            "ll": ll,
            "from": d_dates[0],
            "to": d_dates[-1],
        },
        "weekly": {
            "direction": w_dir,
            "change": w_change,
            "strength": w_strength,
            "bar": strength_bar(w_strength),
            "range": w_range,
            "weeks_up": w_up,
            "weeks_dn": w_dn,
            "from": str(w_starts[0]),
            "to": str(w_starts[-1]),
        },
        # Detailed weeks for display
        "weeks_detail": [
            {
                "start": str(recent_4w["w_start"][i]),
                "open": recent_4w["w_open"][i],
                "high": recent_4w["w_high"][i],
                "low": recent_4w["w_low"][i],
                "close": recent_4w["w_close"][i],
                "days": recent_4w["w_days"][i],
            }
            for i in range(recent_4w.shape[0])
        ],
    }


def analyze_context(profile, price_ref, symbol):
    """Analyze context from the last days of market profile."""
    if profile is None or profile.shape[0] == 0:
        return {}

    last = profile.row(0, named=True)
    prev = profile.row(1, named=True) if profile.shape[0] > 1 else None

    # Price position relative to yesterday's levels
    poc = last["poc"]
    vah = last["vah"]
    val = last["val"]

    if price_ref > vah:
        position = "ABOVE VALUE AREA"
    elif price_ref < val:
        position = "BELOW VALUE AREA"
    elif price_ref > poc:
        position = "BETWEEN POC AND VAH"
    else:
        position = "BETWEEN VAL AND POC"

    # Trend over the last 5 days
    recent_5 = profile.head(5)
    closes = recent_5["day_close"].to_list()
    if len(closes) >= 2:
        trend_pts = closes[0] - closes[-1]  # last - oldest (inverted because desc)
    else:
        trend_pts = 0

    # Recent day types
    types = recent_5["day_type"].to_list()

    # Recent volatility vs median
    ranges = recent_5["day_range"].to_list()
    avg_range = sum(ranges) / len(ranges) if ranges else 0

    return {
        "last_date": last["date"],
        "poc": poc,
        "vah": vah,
        "val": val,
        "va_width": last.get("va_width", vah - val),
        "last_type": last["day_type"],
        "last_range": last["day_range"],
        "last_ib": last["ib_range"],
        "last_direction": last.get("direction", "?"),
        "position": position,
        "trend_5d": trend_pts,
        "types_5d": types,
        "avg_range_5d": avg_range,
        "prev": prev,
    }


def find_zones(ctx, naked_pocs, ib, price_ref, symbol):
    """Identify buy/sell zones and targets."""
    targets = TARGETS[symbol]
    zones = []

    poc = ctx["poc"]
    vah = ctx["vah"]
    val = ctx["val"]

    # === ZONE 1: Value Area Rejection ===
    # Price near VAH → potential sell (return to POC)
    if abs(price_ref - vah) < targets["T1"]:
        zones.append({
            "type": "SELL",
            "name": "VAH Rejection",
            "entry": f"~{vah:.2f}",
            "stop": f"{vah + targets['T1']:.2f}",
            "T1": f"{vah - targets['T1']:.2f}",
            "T2": f"{poc:.2f} (POC)",
            "T3": f"{val:.2f} (VAL)",
            "condition": "Confirmed rejection with Exhaustion or Absorption at VAH",
        })

    # Price near VAL → potential buy (return to POC)
    if abs(price_ref - val) < targets["T1"]:
        zones.append({
            "type": "BUY",
            "name": "VAL Rejection",
            "entry": f"~{val:.2f}",
            "stop": f"{val - targets['T1']:.2f}",
            "T1": f"{val + targets['T1']:.2f}",
            "T2": f"{poc:.2f} (POC)",
            "T3": f"{vah:.2f} (VAH)",
            "condition": "Confirmed rejection with Exhaustion or Absorption at VAL",
        })

    # === ZONE 2: IB Breakout ===
    if ib:
        ib_high = ib["high"]
        ib_low = ib["low"]

        # Bullish breakout
        if price_ref > ib_high:
            zones.append({
                "type": "BUY",
                "name": "IB Breakout Long",
                "entry": f">{ib_high:.2f}",
                "stop": f"{ib_high - targets['T1'] * 0.5:.2f}",
                "T1": f"{ib_high + targets['T1']:.2f}",
                "T2": f"{ib_high + targets['T2']:.2f}",
                "T3": f"Trail ({ib_high + targets['T3']:.2f}+)",
                "condition": "Breakout with Aggression or Initiative_Buy + volume",
            })

        # Bearish breakout
        if price_ref < ib_low:
            zones.append({
                "type": "SELL",
                "name": "IB Breakout Short",
                "entry": f"<{ib_low:.2f}",
                "stop": f"{ib_low + targets['T1'] * 0.5:.2f}",
                "T1": f"{ib_low - targets['T1']:.2f}",
                "T2": f"{ib_low - targets['T2']:.2f}",
                "T3": f"Trail ({ib_low - targets['T3']:.2f}+)",
                "condition": "Breakout with Aggression or Initiative_Sell + volume",
            })

    # === ZONE 3: Naked POC Magnet ===
    for npoc in naked_pocs[:5]:
        dist = npoc["dist"]
        if abs(dist) < targets["T1"]:
            continue  # Too close, already there
        if abs(dist) > targets["T3"] * 2:
            continue  # Too far

        direction = "BUY" if dist > 0 else "SELL"
        zones.append({
            "type": direction,
            "name": f"Naked POC ({npoc['date']})",
            "entry": f"Current price ~{price_ref:.2f}",
            "stop": f"{price_ref + (targets['T1'] * (-1 if direction == 'BUY' else 1)):.2f}",
            "T1": f"{price_ref + (dist * 0.4):.2f}",
            "T2": f"{npoc['poc']:.2f} (Naked POC)",
            "T3": f"Beyond the POC",
            "condition": f"Price attracted to POC {npoc['poc']:.2f} ({abs(dist):.0f} pts, {npoc['age']}d naked, type: {npoc['type']})",
        })

    return zones


def analyze_regimes(regimes_df):
    """Summarize recent regimes."""
    if regimes_df is None or regimes_df.shape[0] == 0:
        return "No regime data"

    regime_counts = regimes_df.group_by("regime").agg(pl.len().alias("n")).sort("n", descending=True)
    dominant = regime_counts.row(0, named=True)

    # Last 6 bars (30 min)
    last_6 = regimes_df.head(6)
    recent_regimes = last_6["regime"].to_list()

    return {
        "dominant_3h": dominant["regime"],
        "dominant_count": dominant["n"],
        "total": regimes_df.shape[0],
        "last_30m": recent_regimes,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════

def format_briefing(symbol, price_ref, ctx, ib, naked_pocs, zones, regime_info, today_candles, trends=None, last_price_time=None):
    """Format the briefing as text."""
    now = datetime.utcnow()
    targets = TARGETS[symbol]
    lines = []

    if last_price_time:
        lines.append(f"  {symbol}@{price_ref:.2f}  {last_price_time.strftime('%H:%M')} UTC  —  {now.strftime('%Y-%m-%d')}")
    else:
        lines.append(f"  {symbol}@{price_ref:.2f}  (manual)  —  {now.strftime('%Y-%m-%d')}")

    # ── TREND ──
    if trends:
        w = trends["weekly"]
        d = trends["daily"]
        lines.append(f"\n{'─' * 65}")
        lines.append(f"  TREND")
        lines.append(f"{'─' * 65}")
        lines.append(f"  Weekly: {w['direction']} {w['bar']}  ({w['change']:+.2f} pts over 4 weeks, {w['weeks_up']}W↑ {w['weeks_dn']}W↓)")
        lines.append(f"          Range: {w['range']:.0f} pts  ({w['from']} → {w['to']})")
        lines.append(f"  Daily : {d['direction']} {d['bar']}  ({d['change']:+.2f} pts over 5d, HH:{d['hh']} LL:{d['ll']})")
        lines.append(f"          Range: {d['range']:.0f} pts  ({d['from']} → {d['to']})")

        # Confluence
        if w["direction"] == d["direction"]:
            lines.append(f"  ▶ W+D confluence: {w['direction']} — strong bias")
        elif w["direction"] == "NEUTRAL" or d["direction"] == "NEUTRAL":
            lines.append(f"  ▶ No clear bias — caution")
        else:
            lines.append(f"  ▶ Divergence W({w['direction']}) vs D({d['direction']}) — possible transition")

        # Recent weeks detail
        lines.append(f"\n  Recent weeks:")
        for wd in trends["weeks_detail"]:
            chg = wd["close"] - wd["open"]
            arrow = "▲" if chg > 0 else "▼"
            rng = wd["high"] - wd["low"]
            lines.append(f"    {wd['start']}  O:{wd['open']:>10.2f}  H:{wd['high']:>10.2f}  L:{wd['low']:>10.2f}  C:{wd['close']:>10.2f}  {arrow}{chg:+.0f}  R:{rng:.0f}  ({wd['days']}d)")

    # ── POSITION ──
    lines.append(f"\n{'─' * 65}")
    lines.append(f"  POSITION")
    lines.append(f"{'─' * 65}")
    lines.append(f"  {ctx['position']}  (vs levels from {ctx['last_date']})")

    # ── KEY LEVELS (YESTERDAY) ──
    lines.append(f"\n{'─' * 65}")
    lines.append(f"  LEVELS ({ctx['last_date']})")
    lines.append(f"{'─' * 65}")
    lines.append(f"  POC : {ctx['poc']:.2f}  {'◄ price near' if abs(price_ref - ctx['poc']) < targets['T1'] else ''}")
    lines.append(f"  VAH : {ctx['vah']:.2f}  {'◄ price near' if abs(price_ref - ctx['vah']) < targets['T1'] else ''}")
    lines.append(f"  VAL : {ctx['val']:.2f}  {'◄ price near' if abs(price_ref - ctx['val']) < targets['T1'] else ''}")
    lines.append(f"  VA  : {ctx['va_width']:.2f} pts")
    lines.append(f"  Type: {ctx['last_type']}  |  Range: {ctx['last_range']:.2f}  |  IB: {ctx['last_ib']:.2f}")

    if ctx["prev"]:
        p = ctx["prev"]
        lines.append(f"  D-2 : POC {p['poc']:.2f}  VAH {p['vah']:.2f}  VAL {p['val']:.2f}  ({p['day_type']})")

    # ── TODAY'S IB ──
    if ib:
        lines.append(f"\n{'─' * 65}")
        lines.append(f"  TODAY'S IB ({IB_HOUR[symbol]}:00-{IB_HOUR[symbol]+1}:00 UTC)")
        lines.append(f"{'─' * 65}")
        lines.append(f"  IB High : {ib['high']:.2f}")
        lines.append(f"  IB Low  : {ib['low']:.2f}")
        lines.append(f"  IB Range: {ib['range']:.2f} pts  (median: ~{116 if symbol == 'NQ' else 12} pts)")
        if ib["range"] > (150 if symbol == "NQ" else 16):
            lines.append(f"  ⚠ WIDE IB — trend day less likely")
        elif ib["range"] < (80 if symbol == "NQ" else 8):
            lines.append(f"  ⚡ NARROW IB — strong breakout potential")

    # ── NAKED POCs NEARBY ──
    if naked_pocs:
        lines.append(f"\n{'─' * 65}")
        lines.append(f"  NAKED POCs (within ±{TARGETS[symbol]['T3'] * 2} pts)")
        lines.append(f"{'─' * 65}")
        for np in naked_pocs[:8]:
            arrow = "↑" if np["dist"] > 0 else "↓"
            lines.append(f"  {np['poc']:>10.2f}  {arrow} {abs(np['dist']):>6.0f} pts  ({np['date']}, {np['age']}d, {np['type']})")

    # ── ORDER FLOW REGIMES ──
    if isinstance(regime_info, dict):
        lines.append(f"\n{'─' * 65}")
        lines.append(f"  ORDER FLOW (last 3h)")
        lines.append(f"{'─' * 65}")
        lines.append(f"  Dominant: {regime_info['dominant_3h']} ({regime_info['dominant_count']}/{regime_info['total']} bars)")
        lines.append(f"  30 min  : {' → '.join(regime_info['last_30m'])}")

    # ── RECENT CONTEXT ──
    lines.append(f"\n{'─' * 65}")
    lines.append(f"  5-DAY CONTEXT")
    lines.append(f"{'─' * 65}")
    lines.append(f"  Types : {', '.join(ctx['types_5d'])}")
    lines.append(f"  Avg range: {ctx['avg_range_5d']:.0f} pts")

    # ── TODAY'S SESSION ──
    if today_candles is not None and today_candles.shape[0] > 0:
        day_high = today_candles["high"].max()
        day_low = today_candles["low"].min()
        day_vol = today_candles["volume"].sum()
        day_delta = (today_candles["ask_vol"].sum() - today_candles["bid_vol"].sum())
        day_range = day_high - day_low
        lines.append(f"\n{'─' * 65}")
        lines.append(f"  TODAY'S SESSION")
        lines.append(f"{'─' * 65}")
        lines.append(f"  Range : {day_low:.2f} — {day_high:.2f}  ({day_range:.2f} pts)")
        lines.append(f"  Volume: {day_vol:,}  |  Delta: {day_delta:+,}")
        lines.append(f"  Candles: {today_candles.shape[0]} × 5m")

    # ── ZONES AND SIGNALS ──
    lines.append(f"\n{'─' * 65}")
    lines.append(f"  ZONES / SIGNALS")
    lines.append(f"{'─' * 65}")

    if not zones:
        lines.append(f"  No clear signal — wait for a setup")
    else:
        for i, z in enumerate(zones, 1):
            color = GREEN if z["type"] == "BUY" else RED
            lines.append(f"\n  [{i}] {z['type']} — {z['name']}")
            lines.append(f"      Entry : {z['entry']}")
            lines.append(f"      Stop  : {z['stop']}")
            lines.append(f"      T1    : {z['T1']}")
            lines.append(f"      T2    : {z['T2']}")
            lines.append(f"      T3    : {z['T3']}")
            lines.append(f"      ✓ If  : {z['condition']}")

    # ── REFERENCE TARGETS ──
    lines.append(f"\n{'─' * 65}")
    lines.append(f"  {symbol} TARGETS")
    lines.append(f"{'─' * 65}")
    lines.append(f"  T1 = {targets['T1']} pts  |  T2 = {targets['T2']} pts  |  T3 = {targets['T3']} pts (trail)")
    lines.append(f"  Buy : T1 {price_ref + targets['T1']:.2f}  T2 {price_ref + targets['T2']:.2f}  T3 {price_ref + targets['T3']:.2f}")
    lines.append(f"  Sell: T1 {price_ref - targets['T1']:.2f}  T2 {price_ref - targets['T2']:.2f}  T3 {price_ref - targets['T3']:.2f}")

    lines.append(f"\n{'=' * 65}")
    lines.append(f"  End of briefing — {now.strftime('%H:%M')} UTC")
    lines.append(f"{'=' * 65}\n")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Eagle Daily Briefing")
    parser.add_argument("symbol", nargs="?", default="NQ", choices=["GC", "NQ"])
    parser.add_argument("--price", type=float, help="Reference price (otherwise: last 5m close)")
    parser.add_argument("--save", action="store_true", help="Save to Reports/Briefings/")
    args = parser.parse_args()

    symbol = args.symbol.upper()

    # Reference price
    last_price_time = None
    price_ref = args.price
    if price_ref is None:
        price_ref, last_price_time = get_current_price(symbol)
    if price_ref is None:
        print(f"  Cannot find price for {symbol}.")
        print(f"  Use --price XXXXX to force.")
        sys.exit(1)

    # Load data
    profile = load_profile(symbol, n_days=10)
    full_profile = load_full_profile(symbol)
    naked_pocs = load_naked_pocs(symbol, price_ref, radius=TARGETS[symbol]["T3"] * 3)
    regimes = load_recent_regimes(symbol, n_bars=36)
    today_candles = get_today_candles(symbol)
    ib = compute_ib(today_candles, symbol)

    # Analysis
    ctx = analyze_context(profile, price_ref, symbol)
    if not ctx:
        print(f"  No market profile data for {symbol}")
        sys.exit(1)

    trends = compute_trends(full_profile, price_ref, symbol)
    zones = find_zones(ctx, naked_pocs, ib, price_ref, symbol)
    regime_info = analyze_regimes(regimes)

    # Briefing
    briefing = format_briefing(symbol, price_ref, ctx, ib, naked_pocs, zones, regime_info, today_candles, trends, last_price_time)

    # Display
    print(briefing)

    # Save
    if args.save:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"briefing_{symbol}_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.txt"
        out_path = REPORTS_DIR / filename
        with open(out_path, "w") as f:
            f.write(briefing)
        print(f"  Saved: {out_path}")


if __name__ == "__main__":
    main()
