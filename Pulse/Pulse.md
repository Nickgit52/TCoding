# Pulse — Institutional tick analysis system for futures

Institutional order flow analysis on Gold (GC) and Nasdaq (NQ).
Tick data → institutional signals → terminal report → AI conversation.

---

## Architecture

```
Sierra Chart (Windows/Parallels)
    │
    ├── .scid files ──→ pulse_sync_scid.py ──→ Data/Scid_Data/
    │                                              │
    └── UDP :11099 ──→ pulse_listen.py ──→ Data/Live_Data/*.csv
                                               │
                            pulse_bridge.py ◄───┘
                            (merges scid + live, filters Front, trims 30d)
                                    │
                            Data/Flux_Data/*_Tick_Flux.parquet
                                    │
                            pulse_institutional.py
                                    │
                            Data/Intel_Data/*_Institutional.parquet
                                    │
                            pulse_report.py → iTerm2 Terminal
```

**Cross-pipeline note (Path B, 2026-05-12).** Pulse now also serves as a `.scid` cache for Eagle's batch pipeline. Eagle's `find_scid()` order is Sierra → `Pulse/Data/Scid_Data/` → `/Volumes/Sam128/TC_Sam128/`. Practical effect: Pulse's daily `sync` keeps Eagle's active-contract reads fresh without Eagle running its own copy step. Pulse itself is unchanged — same scripts, same outputs.

## Scripts

| Script | Command | Role |
|---|---|---|
| pulse_listen.py | `live` | Listen to UDP, write per-tick CSV (zero latency) |
| pulse_sync_scid.py | `sync` | Copy .scid from Sierra Chart (Last/Front/Next) |
| pulse_bridge.py | `bridge` | Merge scid + live → Parquet Front 30 days |
| pulse_institutional.py | `inst` | Detect institutional signals on Flux_Data |
| pulse_volume_profile.py | `vp` | Volume Profile per RTH session (POC, VA, migration) |
| pulse_calibrate.py | — | Threshold calibration (run with `python3 Scripts/pulse_calibrate.py`) |
| pulse_session_deep.py | — | Multi-session calibration (Phase 6) |
| pulse_report.py | `market` / `pr` / `prg` / `prn` | Terminal report with live price + VP + composite |

## Data

| Folder | Content | Format |
|---|---|---|
| Data/Scid_Data/ | Raw Sierra Chart files (Last, Front, Next) | .scid binary |
| Data/Live_Data/ | Real-time ticks (pulse_listen) | .csv |
| Data/Flux_Data/ | Consolidated ticks, Front contract, 30 days | .parquet zstd |
| Data/Intel_Data/ | Institutional signals | .parquet zstd |
| Data/Ticks_Parquet_Training/ | Historical Front data (2.5 years, GC + NQ) | .parquet zstd |
| Data/contracts.json | Active contracts + rollover status | .json |

## Daily workflow

```
1. sync                  (fetch fresh .scid — start of session)
2. live                  (UDP listener — keep running in its own iTerm2 tab)
3. Open a new tab (Ctrl+N) for the following commands
4. maj                   (= bridge && vp && inst && market — rerun for a fresh snapshot)
   or manually:
   bridge                (consolidate data, detect rollover)
   vp                    (Volume Profile — POC, VA, migration)
   inst                  (detect institutional signals)
   market                (terminal report, copy-paste to Claude)
```

Note: `maj` is the French shorthand for "update" — kept as a personal alias preference.

---

## Contract calendar

### NQ — Nasdaq 100 E-mini (CME)

4 contracts per year: H (March), M (June), U (September), Z (December).
Rollover happens approximately 8 days before the third Friday of the expiration month.

| Contract | Month | Front since | Front until | Expiration (3rd Friday) |
|---|---|---|---|---|
| NQH24 | 2024-03 | ~2023-12-14 | ~2024-03-07 | 2024-03-15 |
| NQM24 | 2024-06 | ~2024-03-07 | ~2024-06-06 | 2024-06-21 |
| NQU24 | 2024-09 | ~2024-06-06 | ~2024-09-05 | 2024-09-20 |
| NQZ24 | 2024-12 | ~2024-09-05 | ~2024-12-12 | 2024-12-20 |
| NQH25 | 2025-03 | ~2024-12-12 | ~2025-03-06 | 2025-03-21 |
| NQM25 | 2025-06 | ~2025-03-06 | ~2025-06-05 | 2025-06-20 |
| NQU25 | 2025-09 | ~2025-06-05 | ~2025-09-04 | 2025-09-19 |
| NQZ25 | 2025-12 | ~2025-09-04 | ~2025-12-11 | 2025-12-19 |
| **NQH26** | **2026-03** | ~2025-12-11 | **~2026-03-12** | **2026-03-20** |
| **NQM26** | **2026-06** | **~2026-03-12** | ~2026-06-11 | 2026-06-19 |
| NQU26 | 2026-09 | ~2026-06-11 | ~2026-09-10 | 2026-09-18 |
| NQZ26 | 2026-12 | ~2026-09-10 | ~2026-12-10 | 2026-12-18 |

**State on 2026-05-11: Front = NQM26, Last = NQH26, Next = NQU26**

### GC — Gold COMEX

6 contracts per year: G (February), J (April), M (June), Q (August), V (October), Z (December).
Rollover happens approximately 3 business days before the first notice day (last business day of the month preceding the delivery month).

| Contract | Month | Front since | Front until | First notice day |
|---|---|---|---|---|
| GCG24 | 2024-02 | ~2023-12-27 | ~2024-01-26 | 2024-01-31 |
| GCJ24 | 2024-04 | ~2024-01-26 | ~2024-03-26 | 2024-03-28 |
| GCM24 | 2024-06 | ~2024-03-26 | ~2024-05-28 | 2024-05-31 |
| GCQ24 | 2024-08 | ~2024-05-28 | ~2024-07-26 | 2024-07-31 |
| GCV24 | 2024-10 | ~2024-07-26 | ~2024-09-26 | 2024-09-30 |
| GCZ24 | 2024-12 | ~2024-09-26 | ~2024-11-26 | 2024-11-29 |
| GCG25 | 2025-02 | ~2024-11-26 | ~2025-01-29 | 2025-01-31 |
| GCJ25 | 2025-04 | ~2025-01-29 | ~2025-03-26 | 2025-03-31 |
| GCM25 | 2025-06 | ~2025-03-26 | ~2025-05-28 | 2025-05-30 |
| GCQ25 | 2025-08 | ~2025-05-28 | ~2025-07-29 | 2025-07-31 |
| GCV25 | 2025-10 | ~2025-07-29 | ~2025-09-26 | 2025-09-30 |
| GCZ25 | 2025-12 | ~2025-09-26 | ~2025-11-25 | 2025-11-28 |
| GCG26 | 2026-02 | ~2025-11-25 | ~2026-01-28 | 2026-01-30 |
| **GCJ26** | **2026-04** | ~2026-01-28 | **~2026-03-27** | **2026-03-31** |
| **GCM26** | **2026-06** | ~2026-03-27 | ~2026-05-28 | 2026-05-29 |
| GCQ26 | 2026-08 | ~2026-05-28 | ~2026-07-29 | 2026-07-31 |
| GCV26 | 2026-10 | ~2026-07-29 | ~2026-09-28 | 2026-09-30 |
| GCZ26 | 2026-12 | ~2026-09-28 | ~2026-11-25 | 2026-11-30 |

**State on 2026-05-11: Front = GCM26, Last = GCJ26, Next = GCQ26**

Note: The next rollover (GCM26 → GCQ26) arrives around 2026-05-28.

### Historical volume per contract (total ticks from .scid)

**NQ:**
| Contract | Ticks |
|---|---|
| NQH24 | 35,843,660 |
| NQM24 | 39,197,808 |
| NQU24 | 28,667,083 |
| NQZ24 | 29,589,102 |
| NQH25 | 34,891,706 |
| NQM25 | 34,402,655 |
| NQU25 | 27,829,413 |
| NQZ25 | 34,284,945 |
| NQH26 | 3,137,007 (expiring) |
| NQM26 | 123,801 (new front) |

**GC:**
| Contract | Ticks |
|---|---|
| GCG24 | 5,743,511 |
| GCQ24 | 5,967,914 |
| GCZ24 | 12,662,488 |
| GCG25 | 5,207,060 |
| GCJ25 | 5,498,213 |
| GCM25 | 8,350,733 |
| GCQ25 | 6,525,398 |
| GCG26 | 7,791,161 |
| GCJ26 | 1,242,716 (current front) |
| GCM26 | 120,769 (next) |

---

## Institutional signals

8 signal families detected by pulse_institutional.py:

**IB_BREAK** (UP/DOWN) — Break of the Initial Balance (first hour of RTH). FADE: the breakout is a trap, price returns. UP=86-94%, DOWN=64% GC.

**OPENING_DRIVE** (UP/DOWN) — Direction of the first 30 minutes. FADE: the drive exhausts. UP=62%.

**DELTA_DIVERGENCE** (BULL/BEAR) — Price/cumulative-delta divergence over 200 ticks. REVERSAL. BULL=59-64%.

**ABSORPTION** (BULL/BEAR) — Price rises/falls but delta diverges over 40 ticks. REVERSAL. BULL=56-61%.

**BURST** — Sudden burst of volume + speed. MOMENTUM. ~67% GC.

**LARGE_PRINT** — Tick with abnormally high volume (z-score > 3.0). INFO.

**ICEBERG** — Volume concentrated at the same price level, tick after tick. INFO.

**EXHAUSTION** — Large volume without price movement. INFO.

**STACKED_IMBALANCE** — Consecutive levels with bid/ask imbalance. INFO.

---

## Environment

| Item | Detail |
|---|---|
| Machine | MacBook Air M1, 16 GB RAM |
| OS | macOS Tahoe 26.3.1 |
| Python | 3.9 (python3) |
| Sierra Chart | Windows 11 via Parallels |
| Windows volume | /Volumes/[C] Windows 11/SierraChart/Data |
| Terminal | iTerm2, profile "Pulse" |
| Timezone | UTC-6 (MST) |
| Data | UTC |

---

## Roadmap — Orderflow Intelligence

### Phase 1 — Volume Profile ✅

Script: `pulse_volume_profile.py` (`vp`)

Compute per RTH session: volume per price level (rounded to the tick), POC (Point of Control), Value Area 70% (VAH/VAL), delta per level, VA migration vs previous session. Produces `{symbol}_VolumeProfile.parquet` in Intel_Data.

Integrated into `pulse_report.py`: displays POC, VA, migration, and price position relative to the VA (above / inside / below). Integrated into the `maj` pipeline (bridge && vp && inst && market).

Migrations: HIGHER_VALUE, HIGHER_OVERLAP, LOWER_VALUE, LOWER_OVERLAP, INSIDE, OUTSIDE. Provides spatial context to directional signals.

### Phase 2 — Threshold calibration ✅

Script: `pulse_calibrate.py`

Calibration + grid search results (60 sessions + 30 sessions grid, ~59M + ~268M ticks):

| Signal | GC 120s | NQ 120s | Type | Reliability |
|---|---|---|---|---|
| ABSORPTION_BULL | 62.1% | 60.8% | REVERSAL↓ | ★★★ |
| BURST | 66.7% | ~51% | MOMENTUM | ★★★ (GC) |
| ICEBERG | 55.3% | ~50% | INFO | ★★ (GC) |
| ABSORPTION_BEAR | 50.2% | 47.2% | REVERSAL↑ | ★★ |
| LARGE_PRINT | ~51% | ~50% | INFO | ★ |

Optimized thresholds (grid search):
- BURST_VOL_Z: 2.5 → **2.0** (GC: 52.9%→66.7%, signals doubled)
- ABSORPTION_WINDOW: 30 → **40** (GC: 60.4%→62.1%, NQ: 59.2%→60.8%)
- ICEBERG_MIN_HITS: 4 → **6** (GC: 52.8%→55.3%)

Key findings:
- Absorption = REVERSAL signal, not continuation (directions inverted)
- ABSORPTION_BULL and BURST are the two signals with real edge
- Signal strengthens over time (30s→120s ↑) = real institutional footprint
- BULL/BEAR asymmetry: highs are easier to absorb than lows
- MIN_VOLUME (GC≥5, NQ≥10) eliminates ~90% of noise (vol_avg=1 → vol_avg=9-24)
- BURST works mainly on GC — NQ doesn't have the same burst profile

Commands:
```
python3 Scripts/pulse_calibrate.py                       evaluate current thresholds
python3 Scripts/pulse_calibrate.py --symbol GC           GC only
python3 Scripts/pulse_calibrate.py --sample 30           30 random days (fast)
python3 Scripts/pulse_calibrate.py --sample 30 --grid    grid search on 30 days
```

### Phase 3 — Orderflow signals ✅

Two new signals added and calibrated:

**DELTA_DIVERGENCE** (signal 5) — Best signal of the system.
- DELTA_DIV_BULL: price↑ ≥ threshold (GC: $1, NQ: $5) + negative delta_cum → REVERSAL↓
  - GC: 61.6% — NQ: **64.2%** at 120s ★★★
- DELTA_DIV_BEAR: price↓ + positive delta_cum → REVERSAL↑
  - GC: 53.9% — NQ: 33.8% (inverted! panic selling continues)
- 200-tick window (~2 min) — macro view vs Absorption (40-tick micro)

**STACKED_IMBALANCE** (signal 6) — No directional edge.
- BUY and SELL both below 50% — informational signal only ★

Phase 3 finding — BULL/BEAR asymmetry confirmed:
- BEAR reversal signals don't work on NQ
- NQ panic selling accelerates instead of reversing (33.8% → 66.2% continuation)
- GC is more symmetric (BEAR ~54% vs BULL ~62%)

### Phase 3b — Session Context + Exhaustion ✅

Three new signals added and calibrated (650 sessions, 2.5 years):

**IB_BREAK_UP (FADE)** — The system's best signal.
- When price breaks the IB high, it almost always returns → FADE↓
- GC: **85.6%** — NQ: **93.5%** at 120s ★★★★
- False bullish breakouts are systematic (institutional stop hunting)

**IB_BREAK_DOWN (FADE)** — False bearish breakout, returns upward.
- GC: **64.0%** — NQ: 51.0% at 120s ★★★ (GC)

**OPENING_DRIVE_UP (FADE)** — The bullish drive of the first 30 minutes exhausts.
- GC: **62.0%** — NQ: **62.6%** at 60s ★★★

**OPENING_DRIVE_DOWN** — No edge (~51%). Morning panic selling does not reverse reliably.

**EXHAUSTION** — No edge (~50%). Signal too rare and non-predictive. Kept as INFO.

Phase 3b finding — breakouts are traps:
- IB_BREAK_UP at 86-94% = the most reliable signal, and it's a FADE
- The bull/bear asymmetry strikes again: bullish breakouts are traps, bearish less so
- Measurement at 30/60/120s captures the post-fakeout return well

Complete table of the 14 calibrated signals:

| Signal | GC 120s | NQ 120s | Type | Reliability |
|---|---|---|---|---|
| IB_BREAK_UP | 85.6% | 93.5% | FADE↓ | ★★★★ |
| IB_BREAK_DOWN | 64.0% | 51.0% | FADE↑ | ★★★ (GC) |
| OPENING_DRIVE_UP | 62.0% | 61.2% | FADE↓ | ★★★ |
| DELTA_DIV_BULL | 59.1% | 53.2% | REVERSAL↓ | ★★★ |
| ABSORPTION_BULL | 56.4% | 51.7% | REVERSAL↓ | ★★★ |
| BURST | 50.7% | 48.3% | MOMENTUM | ★★★ (GC) |
| DELTA_DIV_BEAR | 45.6% | 49.7% | REVERSAL↑ | ★★ |
| ABSORPTION_BEAR | 48.0% | 49.8% | REVERSAL↑ | ★★ |
| ICEBERG | 49.9% | 47.1% | INFO | ★★ |
| EXHAUSTION | 50.9% | 45.1% | INFO | ★ |
| OPENING_DRIVE_DOWN | 41.9% | 51.4% | INFO | ★ |
| LARGE_PRINT | 49.5% | 48.9% | INFO | ★ |
| STACKED_IMB_BUY | 45.9% | 49.6% | INFO | ★ |
| STACKED_IMB_SELL | 51.8% | 48.9% | INFO | ★ |

### Phase 4 — Composite score ✅

Three-layer architecture, integrated into `pulse_institutional.py`:

**SESSION layer** — IB_BREAK + OPENING_DRIVE. Persists throughout the RTH session. An IB_BREAK_UP (86-94%) colors the day bearish. Rare signal (1-2/day) but very reliable. Solves the NQ problem (where tick signals have little edge) by providing a directional bias even without flow.

**FLOW layer** — ABSORPTION + DELTA_DIVERGENCE. Time windows (5m, 30m, 2h). What institutional flow is saying right now. Each signal is weighted by its calibrated edge = (hit_rate - 50) / 50, per symbol.

**PULSE layer** — BURST, LARGE_PRINT, ICEBERG, EXHAUSTION, STACKED_IMB. Instantaneous energy. No reliable direction but measures activity intensity.

**Final score** = session_bias + flow_score. Direction + conviction + energy.

**Conviction** = layer alignment. If session and flow point the same way → high conviction. If they diverge → weak signal.

**Verdict** (based on 30m window):
- STRONG: |direction| ≥ 0.50 and conviction ≥ 80%
- MODERATE: |direction| ≥ 0.20 and conviction ≥ 60%
- WEAK / NEUTRAL: otherwise

Calibrated edge per symbol (SIGNAL_EDGE):

| Signal | GC edge | NQ edge | Note |
|---|---|---|---|
| IB_BREAK_UP | 0.71 | 0.82 | 3 sessions (Phase 6) |
| OPENING_DRIVE_UP | 0.41 | 0.27 | ASIA 70.6% GC (Phase 6) |
| IB_BREAK_DOWN | 0.33 | 0.14 | EUR 66.3% GC (Phase 6) |
| DELTA_DIV_BULL | 0.18 | 0.13 | vol≥20 NQ (Phase 5) |
| ABSORPTION_BULL | 0.13 | 0.05 | vol≥20 NQ (Phase 5) |
| Others | ≤0.04 | ≤0.03 | |

### Phase 5 — NQ Deep Exploration ✅

Objective: understand why NQ tick signals have little edge (51-53% vs 56-62% GC) and find the conditions where they work. Script `pulse_nq_deep.py` — 5 hypotheses tested on 635 sessions (268M ticks).

**H1 — Timing**: RTH opening (13:30-14:30 UTC) improves DELTA_DIV_BULL by +3.7pts (56.9% vs 53.2%). Midday (16:00-18:00) brings nothing. Power Hour (19:00-20:00) slightly positive. Likely cause: institutional flows concentrate at the open and close.

**H2 — Volume floor**: vol≥20 is the NQ sweet spot (vs vol≥10 before). DDV_BULL goes from 53.2% to 56.3% (+3pts). Beyond vol≥50, too few signals and degradation.

**H3 — Windows**: Marginal impact. ABS_w=120 gives +1.6pts vs w=30. Not transformative.

**H4 — Confluence**: Major finding. Isolated BULL signals on NQ are sub-50% (ABSORPTION_BULL isolated = 49.3%, DDV_BULL isolated = 51.4%). With confluence (2+ signals ±5min same direction): ABS_BULL 52.6%, DDV_BULL 53.3%. NQ requires confirmation that GC doesn't.

**H5 — Extended horizons**: DDV_BULL stable at 53.5-53.6% up to 10 minutes. No significant improvement with longer horizons.

**Applied in production** (`pulse_institutional.py`):
1. `MIN_VOLUME` NQ raised from 10 → 20
2. `SIGNAL_EDGE` NQ recalibrated: DDV_BULL 0.06→0.13, ABS_BULL 0.03→0.05
3. `NQ_TIME_BOOST`: ×1.4 at the open, ×0.5 at midday
4. `NQ_CONFLUENCE_REQUIRED`: isolated BULL signals = zero edge, only confluent ones count
5. BEAR signals are NOT subject to the confluence gate (isolated BEAR = reliable on NQ)

### Phase 6 — Multi-Session (Asia / Europe / US RTH) ✅

Objective: extend Pulse beyond US RTH to support trading during Europe and Asia hours. Script `pulse_session_deep.py` — 3 tests on 3 sessions, GC (792 dates, 59M ticks) and NQ (769 dates, 268M ticks).

**Sessions:**
- ASIA: 23:00-07:00 UTC (Tokyo → London open)
- EUROPE: 07:00-13:30 UTC (London open → US RTH open)
- US_RTH: 13:30-20:00 UTC (baseline)

**T2 — IB_BREAK_UP FADE: the universal signal (confirmed on all 3 sessions)**

| Session | GC 120s | NQ 120s |
|---|---|---|
| ASIA | 85.5% (n=427) | 91.0% (n=446) |
| EUROPE | 83.7% (n=419) | 89.4% (n=428) |
| US_RTH | 78.1% (n=371) | 90.6% (n=436) |

IB_BREAK_UP is the system's most robust signal — it works everywhere, all the time. IB_BREAK_DOWN is tradable on Europe GC (66.3%) and marginal elsewhere.

**OD_UP FADE**: ASIA GC 70.6%, EUR GC 64.1%, NQ EUR 63.4%. OD_DOWN works nowhere (sub-50%).

**T1 — Tick signals outside RTH**: Europe GC DDV_BULL 57.2%, ABS_BULL 53.7% (comparable to RTH). Asia weaker. NQ tick edge remains weak outside RTH.

**T3 — Volume floors**: Asia GC vol≥10 → DDV_BULL 71.9%. Europe GC vol≥3 → DDV_BULL 60.4%. NQ vol≥20 everywhere.

**Applied in production** (`pulse_institutional.py`):
1. `detect_session_context` now detects IB+OD on all 3 sessions
2. `SIGNAL_EDGE` recalibrated with multi-session edges (OD_UP GC 0.24→0.41, IB_BREAK_DOWN GC 0.28→0.33, NQ IB_BREAK_UP 0.87→0.82)
3. The composite produces session signals even during Europe/Asia hours
4. Signal detail indicates the source session (ASIA/EUROPE/US_RTH)

### Training data

Expected format: Parquet or .scid, continuous Front contract, multiple years.
ML is done elsewhere — Pulse produces structured intelligence, not models.

---

*Living document — last updated on 2026-05-11.*

---

## Rollover convention — mostly manual, with emergency auto-flip

`pulse_bridge.py` detects rollover (status EARLY → APPROACHING → IMMINENT) and reports it in the console. By default the bridge does **not** flip the `front` field in `Data/contracts.json` — manual control over the roll decision is preferred. At a normal roll, edit the file manually before relaunching the bridge.

**Safety-net auto-flip:** if rollover status reaches `IMMINENT` **and** the Next/Front volume ratio exceeds 1.0 (i.e., Next has clearly overtaken Front), the bridge will automatically rewrite `contracts.json` — `last ← front`, `front ← next`, `next ← following contract` — and re-bridge with the new Front. This is a guardrail in case `pulse_sync_scid` hasn't yet caught up. In normal operation (manual sync done in time), this branch is never reached.

Example from 2026-04-15 — the GCJ26 → GCM26 roll had happened on 2026-03-27, but Pulse was still running on GCJ26 (dead contract) because the Pulse .scid files were frozen at 2026-03-19. Fix:

1. `python3 Scripts/pulse_sync_scid.py` — fetch fresh .scid
2. Edit `Data/contracts.json`: `"front": "GCM26"`, `"last": "GCJ26"`, `"next": "GCQ26"`
3. `python3 Scripts/pulse_bridge.py` — rebuild Flux_Data on the correct Front
