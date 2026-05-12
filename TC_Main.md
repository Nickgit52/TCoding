# TC_Main.md — TCoding Operating Procedure and Current State

Companion to `CLAUDE.md` (rules and conventions). This file is current state, operating reference, and session log. When this file and CLAUDE.md disagree, CLAUDE.md wins — fix this file.

Last updated: 2026-05-12

Source: [`github.com/Nickgit52/TCoding`](https://github.com/Nickgit52/TCoding) (private monorepo, since 2026-05-12)

---

## 1. Active Contracts

| Symbol | Last (kept active) | **Front** | Next | Front-roll completed | Next-roll expected |
|---|---|---|---|---|---|
| GC (COMEX) | GCJ26 | **GCM26** | GCQ26 | 2026-03-27 | ~2026-05-28 |
| NQ (CME) | NQH26 | **NQM26** | NQU26 | 2026-03-16 | ~2026-06-11 |

**What stays accessible.** Claude has access to three contracts per symbol at any time: the **front**, the **next**, and the **last** (just-rolled) for ~3 weeks after its roll. The expired contract keeps receiving useful updates during that window, and those updates matter for pondering-session analysis. After 3 weeks the last contract is archived.

**Roll cadence.** GC rolls every 2 months (G/J/M/Q/V/Z). NQ rolls every 3 months (H/M/U/Z). Approximate roll dates are known in advance.

**Roll detection — two phases.**

*Outside the 3-day roll window:* once-daily check is enough. Confirm whether the next roll could happen within the next 3 days. No need for live volume monitoring.

*Inside the 3-day roll window:* active monitoring. When the next contract overtakes the front by daily volume, the switch happens. Claude tells Nick it just switched, identifies the new front, and confirms the just-switched-out contract is now "Last" and stays accessible.

Claude's role: surface roll-window state in the session bilan when inside 3 days; flag the crossover when it happens; never assume a roll happened silently.

GC clean rolls historical: 8 over 792 days (jumps 0.7% to 2.1%). NQ clean rolls historical: 9 over 769 days (jumps 0.9% to 1.4%).

---

## 2. Trading Sessions (UTC)

| Session | Hours | Notes |
|---|---|---|
| Asia | 23:00 – 07:00 | Tokyo → London open |
| Europe | 07:00 – 13:30 | London → US RTH open |
| US RTH | 13:30 – 20:00 | Baseline session |

Nick often trades pre-US RTH (Asia and Europe sessions).

---

## 3. Project Status

**Eagle** — ML baseline (XGBoost on 5m features) calibrated 2026-03-17. Market Profile and naked POC pipeline operational. Order Flow regimes (9 institutional regimes: absorption → sweep) operational. The dashboard at `eagle_server.py` :8888 exists but is **not** the primary live workflow — it has not proven itself in live use. The actual live workflow is iTerm2 reports from Eagle (`market_profile.py`, `orderflow_regimes.py`, `tick_explorer.py`) copy-pasted to Claude, in alternation with Pulse iTerm2 reports (`maj` output, `pr / prg / prn` variants). See `CLAUDE.md` § 7 Session Modes / Live Trading for the exchange protocol.

**Pulse** — 14 calibrated signals over 2.5 years (59M GC + 268M NQ ticks). 3-layer composite (SESSION + FLOW + PULSE). Live UDP listener + batch bridge. Calibration phases 5 (NQ deep) and 6 (multi-session) referenced in `Pulse/Pulse.md`. Rollover convention mostly manual with safety-net auto-flip when IMMINENT.

**Recent activity** — Long quiet stretch between 2026-03-17 and 2026-05-10. Project resumed live observation 2026-05-10/11. See `TC_JOURNAL/Journal_2026-05-11.md`.

---

## 4. Active Datasets

Canonical inventory of data locations, sizes, sync flows, and disaster recovery — see **`TC_Data.md`** at root.

Quick reference (active paths, daily-relevant only):

- Sierra source: `/Volumes/[C] Windows 11/SierraChart/Data/` — Sierra Chart writes here, 6 active contracts (last/front/next per symbol) plus 4 ghost duplicates
- Pulse working copy: `Pulse/Data/Scid_Data/` — 6 .scid, refreshed by `sync`
- Eagle parquets + .scid archive: `/Volumes/Sam128/TC_Sam128/` — 20 .scid (13 GB) + `Ticks_Parquet/` (1.7 GB)
- Derived artifacts: `Eagle/Data/{Candles,Features,Reports,CSV_History}/` on Mac, all reproducible

---

## 5. Common Commands

**Eagle** — run from Mac iTerm2 in `~/Documents/Projets/TCoding/Eagle/`. Use `.venv/bin/python3` directly; `source .venv/bin/activate` fails in this zsh.

`.venv/bin/python3 Scripts/eagle_start.py` — full pipeline (Parallels → volume wait → sync → full pipeline)

`.venv/bin/python3 Scripts/eagle_start.py --no-build --no-analysis` — quick test

`.venv/bin/python3 Scripts/eagle_start.py --dashboard` — pipeline + dashboard

`.venv/bin/python3 Scripts/build_history.py` — smart rebuild (re-reads only last 2 contracts; `--full` for everything)

`.venv/bin/python3 Scripts/build_candles.py` — candles with irreversible roll

`.venv/bin/python3 Scripts/build_features.py` — ML features (21 features + targets)

`.venv/bin/python3 Scripts/market_profile.py` — Market Profile + Naked POCs

`.venv/bin/python3 Scripts/orderflow_regimes.py` — Institutional regimes

`.venv/bin/python3 eagle_server.py` — live dashboard at http://localhost:8888

Eagle venv maintenance:

`.venv/bin/python3 -m pip install <package>` — install

`.venv/bin/pip freeze > requirements.txt` — freeze

Current Eagle packages: polars 1.40.1, polars-runtime-32 1.40.1, tornado 6.5.5.

**Pulse** — system Python (no dedicated venv). Polars is the only external dependency. Shell aliases: `sync`, `live`, `bridge`, `vp`, `inst`, `market`, `maj`, `pr`, `prg`, `prn`, `pulse`.

Daily Pulse workflow: `sync` (start of session) → `live` (UDP listener — keep in dedicated tab) → `Ctrl+N` for new clean tab → `maj` (= `bridge && vp && inst && market`) repeated for fresh snapshots.

---

## 6. ML Calibration — Key Findings (2026-03-17)

| Metric | GC | NQ |
|---|---|---|
| 1h return autocorr | -0.044 (mean-reversion) | +0.013 (neutral) |
| 1h vol persistence | 0.178 (moderate) | 0.659 (strong) |
| Price impact (delta→ret) | 0.354 (strong) | 0.117 (weak) |
| 1m kurtosis | 308 | 120 |
| 1h skewness | -3.52 (violent drops) | +0.67 (violent rallies) |
| % positive days | 54.2% | 55.5% |

(Also in CLAUDE.md § 6 for fast access during live analysis.)

---

## 7. Market Profile Calibration (2026-03-17)

| Metric | GC (645d) | NQ (635d) |
|---|---|---|
| Dominant type | Normal Var 42% | Normal Var 51% |
| Trend days | 31.9% | 23.3% |
| Median IB | 12.3 pts | 116 pts |
| Median RTH range | 26.6 pts | 250 pts |
| Naked POC fill ≤1d | 49% | 63% |
| Naked POC fill ≤5d | 78% | 85% |

---

## 8. Technical Architecture

Sierra Chart (Windows / Parallels) writes `.scid` files to `/Volumes/[C] Windows 11/SierraChart/Data/`. Eagle's `Scripts/build_history.py` reads from there (and falls back to `Pulse/Data/Scid_Data/` then `/Volumes/Sam128/TC_Sam128/` per Path B `find_scid()` order) and writes parquets to `/Volumes/Sam128/TC_Sam128/Ticks_Parquet/`. Sierra also broadcasts live ticks on UDP :11099 via the `argonudp_ARM64.dll` custom study; `eagle_server.py` (Tornado :8888) listens, re-aggregates server-side, and pushes to `dashboard.html` over WebSocket. For Pulse live ticks, `argonudp.cpp` (Sierra Chart custom study source) must be compiled and attached to a chart; `pulse_listen.py` is the consumer.

---

## 9. Latest Journal

All session journals live in `TC_JOURNAL/` (folder created 2026-05-12). At boot routine, the actual latest journal is whichever file in that folder sorts last by ISO date — do not rely on the filename below to be current.

Latest at time of last TC_Main.md update: `Journal_2026-05-11.md` — pre-US RTH session, GC/NQ rotation hypothesis. Snapshots 1–25 from 11:09 to 14:30 UTC. Key takeaway: structural map BEFORE snapshot analysis. See `CLAUDE.md` § 7 and the `feedback_structural_map_first` memory.

---

## 10. Open Questions

`TC_REVIEW/Q4_Nick.md` — Q1 (CLAUDE / TC_Main rename) RESOLVED 2026-05-12. No open questions currently.

**Archive policy.** Resolved questions stay in `Q4_Nick.md` with a `RESOLVED YYYY-MM-DD` tag indefinitely (low volume — won't bloat). If the file ever exceeds ~50 entries, rotate older resolved entries to `TC_JOURNAL/Q4_Nick_archive_YYYY.md` and reset the working file to open questions only.

---

## 11. Recent Decisions

- **2026-05-12 (evening)** — TCoding put under git + pushed to GitHub. Monorepo at `github.com/Nickgit52/TCoding` (private). Eagle/.git and Pulse/.git (no remote, 3 commits each — trivial history) moved to TC_DISPOSE/2026-05-12/ before init. Root `.gitignore` added. Single initial commit captures the full Path B state.
- **2026-05-12 (evening)** — `TC_Data.md` promoted to root (was `TC_REVIEW/TC_Data_DRAFT.md`). § 4 Active Datasets collapsed to a 4-bullet quick reference + pointer to TC_Data. CLAUDE.md § 3 + § 8 updated to reference TC_Data.
- **2026-05-12 (late afternoon)** — Path B executed for Eagle. 6 scripts refactored to absolute paths (`build_history.py`, `eagle_server.py`, `eagle_start.py`, `build_candles.py`, `tick_explorer.py`, `explore_ml.py`). `find_scid()` order: Sierra → Pulse/Data/Scid_Data → TC_Sam128. Sync logic removed from Eagle (Pulse owns it). Eagle parquets moved from Storage/220_Offload/ to TC_Sam128/Ticks_Parquet/. Dangling Ticks_Parquet symlink moved to TC_DISPOSE. Awaiting smart-rebuild test from iTerm2.
- **2026-05-12 (afternoon)** — Sam128 restructured. `TCoding/Raw_Scid` symlink removed entirely. Former `/Volumes/Sam128/220_Offload/` moved into `/Volumes/Sam128/Storage/220_Offload/`. 20 .scid files (full historical set + active overlap) consolidated into new `/Volumes/Sam128/TC_Sam128/` via `mv`. Eagle's `Ticks_Parquet` symlink now dangles (pre-move path); Eagle batch pipeline non-functional pending fix. Pulse verified intact end-to-end (`sync` + `maj` both passed; Pulse uses no Sam128 paths).
- **2026-05-12 (morning)** — Renamed `AI_Pref_TCoding.md` → `CLAUDE.md` and `TCoding.md` → `TC_Main.md`. Adopted MR3-style CLAUDE / Main pairing with explicit precedence. Created `TC_SCAN`, `TC_REVIEW`, `TC_DISPOSE`, `TC_JOURNAL` workflow folders. Created `Roll.csv` (forward + historical roll schedule, GC + NQ through 2027), `Abbreviation.md` (code reference for Pulse signals, Eagle regimes, MP codes, aliases), `Nick_Typo.csv` (typo log, seeded with 5 MR3-era entries). Locked structural-map-first protocol for live trading (CLAUDE.md § 7). Decided NOT to rename `Eagle/`, `Pulse/` — machine efficiency over naming aesthetic; rename cost (venv rebuild + 16 script docstrings + zsh aliases + Sierra Chart study + symlinks) outweighs consistency benefit.
- **2026-04-15** — Pulse calendar-based rollover detection corrected. Safety-net auto-flip added.
- **2026-04** — Sam128 offload established for `Raw_Scid` and Eagle's `Ticks_Parquet` via symlinks.
- **2026-03-27** — GC roll completed (GCJ26 → GCM26, detected automatically).
- **2026-03-16** — NQ roll completed (NQH26 → NQM26, detected automatically).
- **2026-03-17** — ML baseline calibration captured for GC and NQ.

---

## 12. Session Log

- **2026-05-12 (evening)** — TCoding under git as monorepo, pushed to `github.com/Nickgit52/TCoding` (private). gh auth set up. Eagle/Pulse sub-repos discarded into TC_DISPOSE.
- **2026-05-12 (evening)** — TC_Data.md promoted to root from TC_REVIEW. TC_Main § 4 collapsed to pointer. CLAUDE.md updated to reference TC_Data.
- **2026-05-12 (late afternoon)** — Path B executed. 6 Eagle scripts refactored to absolute paths; sync logic removed (Pulse owns it now). Eagle parquets moved to TC_Sam128/Ticks_Parquet/. Dangling Ticks_Parquet symlink moved to TC_DISPOSE. All 6 scripts pass syntax check. Awaiting iTerm2 smart-rebuild test.
- **2026-05-12 (afternoon)** — Sam128 restructure executed. `Raw_Scid` symlink removed, `220_Offload/` moved into `Storage/`, 20 .scid files consolidated into `TC_Sam128/`. Pulse end-to-end verified intact (`sync` + `maj` passed). Governance docs (CLAUDE.md, TC_Main.md, Eagle/note_ml.md) updated to reflect new Sam128 layout and flag the broken Eagle `Ticks_Parquet` symlink.
- **2026-05-12 (morning)** — Restructured TC governance. New CLAUDE.md + TC_Main.md promoted to root. TC_SCAN / TC_REVIEW / TC_DISPOSE / TC_JOURNAL workflow folders created. Roll.csv + Abbreviation.md + Nick_Typo.csv added at root. Old `AI_Pref_TCoding.md` and `TCoding.md` moved to TC_DISPOSE/2026-05-12/. Decided not to rename Eagle / Pulse (machine efficiency).
- **2026-05-11** — Live observation session, GC/NQ pre-US RTH. Lesson learned: structural map before snapshot analysis. See `TC_JOURNAL/Journal_2026-05-11.md`.

---

## 13. Roadmap / Next Steps

- Normalize GC features by price (% instead of absolute points) to fix overfitting.
- Per-contract stats (bid/ask lifetime, efficiency ratio, large trades).
- Walk-forward validation.
- Live dashboard indicators (VWAP, cumulative delta, naked POCs surfaced visually).
- Compile `argonudp.cpp` if Pulse live UDP needs it.
- Consider offloading `Pulse/Data/Ticks_Parquet_Training/` (1.7 GB) to Sam128 if Mac space gets tight.
- Optimize structural-map pull (script that surfaces yesterday's POC/VAH/VAL + naked POCs + gap + IB extremes in one command at session start).
- **Audit hardcoded paths and venv state** (queued 2026-05-12 — Pondering-mode task). Items to review: Eagle `.venv/` health and Python version alignment; 16 `.py` scripts with hardcoded path docstrings (Pulse `pulse_listen.py`, `pulse_sync_scid.py`, `pulse_institutional.py` carry full absolute paths); zsh aliases (`sync`, `live`, `bridge`, `vp`, `inst`, `market`, `maj`, `pr`, `prg`, `prn`, `pulse`) — verify each still works and points at the right cwd; `Eagle/Data/Ticks_Parquet → /Volumes/Sam128/...` symlink integrity; `argonudp.cpp` Sierra Chart study compile state and output paths; `Raw_Scid` reference in `build_history.py` line 39.

---

## 14. Archived / Reference Projects

- **Argon V15** — github.com/Nickgit52/Argon-V15. Phase 1 complete, on hold. 351M ticks (NQ + GC) processed. Operational modules: ML clusters, historical levels, solidity score, dashboard, live pipeline.
- **Krypton V7** — github.com/Nickgit52/Krypton-V7. Modules 3-6 to recover.
- **220_Recreate** — Tick data pipeline reconstruction. Integrated into Eagle; reference scripts kept (`live_candles.py`, `live_recorder.py`, `udp_receiver.py`).
