# Abbreviation.md — TCoding Code Reference

Code reference for TC. Read on demand to disambiguate codes seen in journals, terminal reports, scripts, or analysis. Sourced from `Pulse/Pulse.md`, `Eagle/note_ml.md`, and `Pulse/Scripts/pulse_sync_scid.py`. Update when new signals or regimes are added.

---

## Symbols and Exchanges

| Code | Meaning |
|---|---|
| NQ | Nasdaq 100 E-mini Futures (CME) |
| GC | Gold Futures (COMEX) |

## Contract Month Codes

| Code | Month | Used by |
|---|---|---|
| F | January | (not used) |
| G | February | GC |
| H | March | NQ |
| J | April | GC |
| K | May | (not used) |
| M | June | GC, NQ |
| N | July | (not used) |
| Q | August | GC |
| U | September | NQ |
| V | October | GC |
| X | November | (not used) |
| Z | December | GC, NQ |

NQ has 4 contracts/year (H, M, U, Z). GC has 6 contracts/year (G, J, M, Q, V, Z). Year is appended as 2-digit (e.g., GCM26 = Gold June 2026).

## Trading Sessions (UTC)

| Code | Hours | Description |
|---|---|---|
| ASIA | 23:00 – 07:00 | Tokyo → London open |
| EUROPE | 07:00 – 13:30 | London open → US RTH open |
| US_RTH | 13:30 – 20:00 | US Regular Trading Hours (baseline) |
| ETH | (everything else) | Extended Trading Hours |
| IB | first hour of RTH | Initial Balance |

---

## Pulse Signals (14 calibrated)

Each signal has a Type tag indicating how to trade it: **FADE↓** = trade short against an upward move; **FADE↑** = trade long against a downward move; **REVERSAL↓** / **REVERSAL↑** = expect price to reverse in indicated direction; **MOMENTUM** = trade with the move; **INFO** = no directional edge, informational only.

Reliability stars: **★★★★** exceptional · **★★★** strong, tradable · **★★** moderate, conditional · **★** informational. Calibrated edges per Pulse.md Phase 3b table.

| Signal | Type | GC 120s | NQ 120s | Stars | Notes |
|---|---|---|---|---|---|
| IB_BREAK_UP | FADE↓ | 85.6% | 93.5% | ★★★★ | Best signal in the system; bullish IB breakouts are systematic stop hunts |
| IB_BREAK_DOWN | FADE↑ | 64.0% | 51.0% | ★★★ (GC) | Bearish IB break; tradable mainly on GC EUROPE |
| OPENING_DRIVE_UP | FADE↓ | 62.0% | 61.2% | ★★★ | First-30-min bullish drive exhausts |
| OPENING_DRIVE_DOWN | INFO | 41.9% | 51.4% | ★ | No edge; morning panic does not reverse reliably |
| DELTA_DIV_BULL | REVERSAL↓ | 59.1% | 53.2% | ★★★ | Price↑ + delta_cum negative; 200-tick macro window |
| DELTA_DIV_BEAR | REVERSAL↑ | 45.6% | 49.7% | ★★ | Price↓ + delta_cum positive; weak on NQ (panic continues) |
| ABSORPTION_BULL | REVERSAL↓ | 56.4% | 51.7% | ★★★ | Price rises but adverse delta absorbed; 40-tick micro |
| ABSORPTION_BEAR | REVERSAL↑ | 48.0% | 49.8% | ★★ | Symmetric to BULL but weaker |
| BURST | MOMENTUM | 50.7% | 48.3% | ★★★ (GC) | Sudden volume + speed burst; works on GC, not NQ |
| LARGE_PRINT | INFO | 49.5% | 48.9% | ★ | Tick with vol z-score > 3 |
| ICEBERG | INFO | 49.9% | 47.1% | ★★ | Repeated hits at same price = hidden order |
| EXHAUSTION | INFO | 50.9% | 45.1% | ★ | Large volume without price movement |
| STACKED_IMB_BUY | INFO | 45.9% | 49.6% | ★ | Consecutive levels with bid imbalance |
| STACKED_IMB_SELL | INFO | 51.8% | 48.9% | ★ | Consecutive levels with ask imbalance |

### Pulse Composite Layers (Phase 4)

| Layer | Signals | Role |
|---|---|---|
| SESSION | IB_BREAK + OPENING_DRIVE | Persists through RTH session; sets directional bias |
| FLOW | ABSORPTION + DELTA_DIVERGENCE (5m/30m/2h windows) | What institutional flow is doing right now |
| PULSE | BURST + LARGE_PRINT + ICEBERG + EXHAUSTION + STACKED_IMB | Instantaneous energy / activity intensity |

Final score = `session_bias + flow_score`. Conviction = layer alignment percentage.

### Composite Verdict (30m window)

| Verdict | Direction | Conviction |
|---|---|---|
| STRONG | \|dir\| ≥ 0.50 | ≥ 80% |
| MODERATE | \|dir\| ≥ 0.20 | ≥ 60% |
| WEAK / NEUTRAL | otherwise | — |

### Pulse Metric Names

| Metric | Meaning |
|---|---|
| dir | Composite directional score (signed: + bullish, − bearish) |
| flow 5m / 30m / 2h | Sum of flow signal edges over the time window |
| energy 5m / 30m | Total signal count weighted by edge magnitude |
| edge rate | Ratio of signals with positive edge / total signals in window |
| conviction | Layer alignment percentage |
| composite | Final score = session_bias + flow_score |
| edge | (hit_rate − 50) / 50, per symbol per signal — used in FLOW weighting |

---

## Eagle Order Flow Regimes (9, per note_ml.md § 7)

Detected per 5m candle by `orderflow_regimes.py`, based on rolling z-scores (1h fast, 3h slow).

| Regime | GC | NQ | Signal |
|---|---|---|---|
| Absorption | 0.8% | 0.2% | Invisible wall — large volume, price stuck, opposing delta |
| Compression | 17.6% | 10.2% | Range contracts, volume falls — loading spring |
| Distribution | 1.5% | 2.5% | Large volume + indecision (body < 30% of range) |
| Aggression | 1.6% | 3.2% | Delta + range + volume explode together |
| Exhaustion | 3.1% | 4.1% | Volume climax + immediate reversal |
| Iceberg | 9.5% | 0.5% | Repeated hits at same price — hidden order |
| Sweep | 0.3% | 0.3% | Crosses levels then reverses — stop hunt |
| Initiative | 9.7% | 13.6% | Trading outside Value Area with conviction (delta confirms) |
| Rotation | 12.7% | 11.0% | Many levels visited — liquidity search |

---

## Market Profile

| Code | Meaning |
|---|---|
| POC | Point of Control — highest-volume price level of the session |
| VAH | Value Area High (top of 70% volume area) |
| VAL | Value Area Low (bottom of 70% volume area) |
| VA | Value Area (price range containing 70% of session volume) |
| IB | Initial Balance — first hour of RTH price range |
| Naked POC | POC from prior session that price has not yet revisited |
| RTH | Regular Trading Hours |
| ETH | Extended Trading Hours |

### Day Types (Market Profile)

| Type | GC | NQ | Characteristic |
|---|---|---|---|
| Normal Var | 41.9% | 50.9% | Moderate IB, one-sided extension |
| Trend | 31.9% | 23.3% | Narrow IB, strong extension (>2.5× IB) |
| Non-Trend | 12.4% | 7.7% | Very narrow total range |
| Normal | 9.9% | 6.6% | Wide IB, little extension |
| Neutral | 3.9% | 11.5% | Extensions on both sides |

### VA Migration (Pulse Volume Profile)

| Code | Meaning |
|---|---|
| HIGHER_VALUE | VA shifted up |
| HIGHER_OVERLAP | VA shifted up but overlaps previous |
| LOWER_VALUE | VA shifted down |
| LOWER_OVERLAP | VA shifted down but overlaps previous |
| INSIDE | VA fully inside previous |
| OUTSIDE | VA fully outside previous (rare) |

---

## Rollover States

### Live status (Pulse `contracts.json` — `rollover.status`)

| State | Meaning |
|---|---|
| EARLY | Next/Front volume ratio is low; roll far away |
| APPROACHING | Ratio rising; within 1–2 weeks of expected roll |
| IMMINENT | Within days; triggers safety-net auto-flip if ratio > 1.0 |

### Schedule status (`Roll.csv` — `status` column)

| Status | Meaning |
|---|---|
| historical | Rolled out, no longer active |
| last | Most recent ex-front; kept accessible ~3 weeks post-roll |
| front | Current front contract |
| next | First contract after front |
| upcoming | Future contracts beyond next |

---

## Shell Aliases (Pulse, zsh)

| Alias | Script / command | Role |
|---|---|---|
| `sync` | `pulse_sync_scid.py` | Fetch fresh .scid from Sierra Chart (Last/Front/Next) |
| `live` | `pulse_listen.py` | UDP listener — keep in dedicated tab |
| `bridge` | `pulse_bridge.py` | Merge .scid + live → Front parquet (30 days) |
| `vp` | `pulse_volume_profile.py` | Volume Profile per RTH session (POC, VA, migration) |
| `inst` | `pulse_institutional.py` | Detect institutional signals + composite |
| `market` | `pulse_report.py` | Terminal report (both symbols) |
| `maj` | `bridge && vp && inst && market` | Full snapshot refresh — French *mise à jour* |
| `pr` | `pulse_report.py` | Terminal report (both symbols) |
| `prg` | `pulse_report.py --symbol GC` | Terminal report (GC only) |
| `prn` | `pulse_report.py --symbol NQ` | Terminal report (NQ only) |
| `pulse` | `cd ~/Documents/Projets/TCoding/Pulse/` | Navigation shortcut |

## Eagle Commands (no aliases — direct .venv/bin/python3 invocations)

| Command | Role |
|---|---|
| `eagle_start.py` | Full pipeline (Parallels → sync → build → analyze) |
| `build_history.py` | .scid → ticks parquet (smart rebuild; `--full` for everything) |
| `build_candles.py` | Ticks → candles 1m/5m/15m/1h/1d (irreversible roll) |
| `build_features.py` | Candles → ML features (21 features + 3 targets) |
| `market_profile.py` | Market Profile + Naked POCs |
| `orderflow_regimes.py` | 9 institutional regimes |
| `tick_explorer.py` | Tick exploration (filters, large trades, hourly profile) |
| `explore_ml.py` | QC + ML stats + tick microstructure (v4 streaming) |
| `train_baseline.py` | XGBoost baseline (volatility prediction) |
| `daily_briefing.py` | Text briefing (orphan — under review) |
| `market_brief.py` | Orchestrator + briefing (orphan — under review) |
| `eagle_server.py` | Tornado dashboard server (port 8888) |

---

## Change Log

- 2026-05-12 — Created. Sourced from `Pulse/Pulse.md` (Phases 3–6) and `Eagle/note_ml.md` (§ 6 Market Profile, § 7 Order Flow Regimes).
