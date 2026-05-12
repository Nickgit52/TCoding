# TC_Data.md — TCoding Data Management

Read on demand when a question concerns: data storage locations, data flows between pipelines, archive policy, sync conventions, or disaster recovery. Companion to `CLAUDE.md` (rules) and `TC_Main.md` (current state). When this file disagrees with `TC_Main.md` on a data topic, **this file wins** — fix `TC_Main.md`.

---

## 1. Purpose

Single source of truth for data topics in TCoding. The conversation that produced Path B (2026-05-12) crystallized into this doc.

Out of scope: trading signals (see `Abbreviation.md`), ML calibration (see `Eagle/note_ml.md` and `Pulse/Pulse.md`), session conventions (see `CLAUDE.md`).

---

## 2. Physical locations

| Location | Role | Owner | Notes |
|---|---|---|---|
| `/Volumes/[C] Windows 11/SierraChart/Data/` | Source of truth — Sierra writes ticks from Denali feed | Sierra Chart | Active contracts only (last/front/next per symbol, 6 GC+NQ files). Sierra purges deeper history. Also has dead date-style ghost duplicates from Apr 15 — disposal candidate |
| `/Volumes/Sam128/TC_Sam128/` | TC home on Sam128 — deep archive | Mac scripts | 20 .scid files (~13 GB historical archive) + `Ticks_Parquet/` subfolder |
| `/Volumes/Sam128/TC_Sam128/Ticks_Parquet/` | Eagle's tick parquets + meta JSON | Eagle `build_history.py` | `GC_ticks.parquet` (366 MB, 60M ticks) · `NQ_ticks.parquet` (1.4 GB, 270M ticks) · `{symbol}_meta.json` (per-contract tick counts for smart-rebuild) |
| `Pulse/Data/Scid_Data/` | Pulse working copy of last/front/next | Pulse `pulse_sync_scid.py` | 6 .scid files (~351 MB), kept fresh by `sync` alias |
| `Pulse/Data/Live_Data/` | Continuous UDP tick stream from Sierra | Pulse `pulse_listen.py` | `{GC,NQ}_Live.csv` append-only, populated by `live` alias |
| `Pulse/Data/Flux_Data/` | 40-day Front parquet (Scid_Data + Live_Data merged) | Pulse `pulse_bridge.py` | Produced on each `maj` |
| `Pulse/Data/Intel_Data/` | Per-symbol Institutional + Composite parquets | Pulse `pulse_institutional.py` | Produced on each `maj` |
| `Pulse/Data/Ticks_Parquet_Training/` | Historical Front (2.5 years) for calibration | Pulse `pulse_calibrate.py` | 1.7 GB on Mac, Sam128 offload candidate |
| `Eagle/Data/Candles/` | OHLCV candles (1m/5m/15m/1h/1d) | Eagle `build_candles.py` | Derived from Ticks_Parquet |
| `Eagle/Data/Features/` | ML features (21 features + 3 targets per symbol) | Eagle `build_features.py` | Derived from Candles |
| `Eagle/Data/Reports/` | Market Profile + Order Flow Regimes | Eagle `market_profile.py` + `orderflow_regimes.py` | Derived from Ticks_Parquet |
| `Eagle/Data/CSV_History/` | Continuous price history CSV | Eagle (legacy) | 14 MB |
| `/Volumes/Sam128/Storage/220_Offload/` | Empty shell post-restructure | (none) | Disposal candidate — contains only empty `Raw_Scid/` and historic `README.md` |
| `/Volumes/Sam128/Storage/` | Classify-later pile | Nick | Backups, predecessor folders, USB image. Not part of TC pipelines |

---

## 3. Data classes and placement rule

| Class | Lives on | Rationale |
|---|---|---|
| Source of truth | Sierra (Parallels) | Sierra writes; no choice |
| Active working copies | Mac internal | Fast access; no Sam128 dependency at session start for Pulse |
| Deep archive | Sam128 only | Cheap large storage; rarely accessed; **irreplaceable** for the 16 historical contracts (Sierra purged them) |
| Derived artifacts (<1 GB) | Mac internal | Quick rebuild; doesn't justify Sam128 round-trip |
| Derived artifacts (≥1 GB) | Sam128 | Eagle's GC+NQ tick parquets (1.7 GB total) post-Path-B |
| Live operational data | Mac internal | Pulse's Live_Data, Flux_Data, Intel_Data — small, transient |

**Guiding principle**: minimize Mac internal SSD usage. What doesn't need fast access offloads to Sam128.

---

## 4. Data flows

Sierra Chart (Denali feed, Parallels) is the single root source. Two consumers:

**Pulse** — micro / live pipeline
- Sierra `.scid` files → `Pulse/Data/Scid_Data/` (via `pulse_sync_scid.py`, alias `sync`)
- Sierra UDP :11099 → `Pulse/Data/Live_Data/{GC,NQ}_Live.csv` (via `pulse_listen.py`, alias `live`)
- `Scid_Data/` + `Live_Data/` → `Pulse/Data/Flux_Data/{GC,NQ}_Flux.parquet` (via `pulse_bridge.py`, alias `bridge`)
- `Flux_Data/` → `Intel_Data/` (via `pulse_institutional.py`, alias `inst`)
- `Flux_Data/` → terminal snapshot (via `pulse_volume_profile.py` + `pulse_report.py`, aliases `vp` + `market`/`pr`/`prg`/`prn`)
- Full snapshot orchestrator: alias `maj` (= `bridge && vp && inst && market`)

**Eagle** — macro / structural pipeline (post-Path-B)
- `find_scid()` search order: Sierra → `Pulse/Data/Scid_Data/` → `/Volumes/Sam128/TC_Sam128/`
- `.scid` (active + historical) → `/Volumes/Sam128/TC_Sam128/Ticks_Parquet/{GC,NQ}_ticks.parquet` (via `build_history.py`)
- `Ticks_Parquet/` → `Eagle/Data/Candles/` (via `build_candles.py`)
- `Candles/` → `Eagle/Data/Features/` (via `build_features.py`)
- `Ticks_Parquet/` → `Eagle/Data/Reports/Market_Profile/` (via `market_profile.py`)
- `Ticks_Parquet/` → `Eagle/Data/Reports/Order_Flow/` (via `orderflow_regimes.py`)
- Orchestrator: `eagle_start.py`

---

## 5. Smart-rebuild and archive policy

### Eagle `build_history.py` — smart vs full

**Smart mode** (default): re-reads only the last 2 contracts per chain (active "last" + "front"). The 16 historical contracts in TC_Sam128 are read **once** during initial build and live inside the parquet thereafter. Each daily run touches at most 4 .scid files (2 per symbol).

**Full mode** (`--full`): re-reads all 10 contracts per symbol from .scid. Reserved for:
- Initial build (first time creating the parquet)
- Correcting a historical contract
- Schema migration

Per-symbol `{symbol}_meta.json` records the tick count per contract from the last build. `needs_rebuild()` compares the current .scid tick counts of the last 2 contracts against the meta to decide whether to rebuild.

### .scid lifecycle by status

- **Active fronts** (e.g., GCM26, NQM26 today): Sierra writes continuously. Pulse syncs on `sync`. Eagle reads through `find_scid()`.
- **Nexts** (GCQ26, NQU26 today): same, light volume. Become front on roll.
- **Lasts** (GCJ26, NQH26 today): expired but accessible. Mostly frozen; Sierra trickles tiny adjustments on GC (e.g., GCJ26 +158 ticks between Apr 15 and 2026-05-12) but not NQ.
- **Historicals** (GCG24–GCG26, NQH24–NQZ25): never change. Live only in TC_Sam128. Sierra has purged them.

### Sierra retention

Sierra holds only the active 6 GC+NQ contracts plus dead date-style duplicates from Apr 15. **Sierra is not an archive**: the 16 historical contracts in TC_Sam128 cannot be re-fetched from Sierra.

---

## 6. Capacity (as of 2026-05-12 post-Path-B)

| Volume | Total | Used | Free | Notes |
|---|---|---|---|---|
| Mac internal SSD | 228 GB | ~176 GB | ~52 GB | OS + apps + TC Pulse data |
| Sam128 (Samsung 128 GB USB-C) | 120 GB | ~62 GB | ~58 GB | TC archive + Storage pile |

### Sam128 — TC-relevant breakdown

- `TC_Sam128/` ~15 GB: 20 .scid (13 GB) + Ticks_Parquet (1.7 GB)
- `Storage/220_Offload/` <10 KB: empty shell, disposal candidate
- `Storage/` (excluding 220_Offload) ~47 GB: backups, predecessor folders, USB image — not TC

### Mac internal — TC-relevant breakdown

- `Pulse/Data/Scid_Data/` ~351 MB: 6 active .scid
- `Pulse/Data/Ticks_Parquet_Training/` ~1.7 GB: offload candidate
- `Pulse/Data/{Live,Flux,Intel}_Data/` <50 MB: transient
- `Eagle/Data/{Candles,Features,Reports,CSV_History}/` <100 MB: derived

### Trigger points for offload

- Mac internal free < 20 GB → consider offloading `Pulse/Data/Ticks_Parquet_Training/` to Sam128
- Sam128 free < 10 GB → archive or prune `Storage/`

---

## 7. Sync conventions per pipeline

### Pulse — owns .scid syncing

- Source: Sierra `/Volumes/[C] Windows 11/SierraChart/Data/`
- Destination: `Pulse/Data/Scid_Data/`
- Trigger: alias `sync` (manual, once per session)
- Scope: last + front + next per symbol (6 files)
- Logic: incremental — skip if source is same size or older than dest
- Side effect: updates `Pulse/Data/contracts.json` (last/front/next config consumed by bridge)

### Pulse — live tick stream

- Source: UDP :11099 from Sierra's argonudp study
- Destination: `Pulse/Data/Live_Data/{GC,NQ}_Live.csv` (append-only, no buffer)
- Trigger: alias `live` (kept in dedicated iTerm2 tab)

### Pulse — bridge merge

- Inputs: `Pulse/Data/Scid_Data/*.scid` + `Pulse/Data/Live_Data/*.csv`
- Output: `Pulse/Data/Flux_Data/{GC,NQ}_Flux.parquet` (40-day Front only)
- Trigger: alias `bridge`, runs as first step of `maj`

### Eagle — no sync ownership (Path B, 2026-05-12)

- Eagle no longer copies from Sierra
- `find_scid()` search order: Sierra → `Pulse/Data/Scid_Data/` → `/Volumes/Sam128/TC_Sam128/`
- Sync dependency delegated to Pulse's `sync`

### Eagle — `build_history` smart-rebuild

- Triggered by `eagle_start.py` or directly via `python3 Scripts/build_history.py`
- Reads existing parquet at `/Volumes/Sam128/TC_Sam128/Ticks_Parquet/`
- Re-reads last 2 contracts from .scid via `find_scid()`
- Writes back to same location
- Updates `{symbol}_meta.json`

---

## 8. Disaster recovery

### Replicated via GitHub (since 2026-05-12)

The full TCoding tree is version-controlled at `github.com/Nickgit52/TCoding` (private monorepo). Covers all code (Eagle + Pulse scripts), all governance docs (CLAUDE.md, TC_Main.md, TC_Data.md, Abbreviation.md, Roll.csv, Nick_Typo.csv), session journals (TC_JOURNAL/), and the operator-review queue (TC_REVIEW/). Cadence: per `git push`. Disaster recovery for these layers = `git clone`.

### Reproducible from upstream data

- Eagle derived artifacts: candles, features, reports — re-run the pipeline scripts.
- Pulse derived artifacts: Flux_Data, Intel_Data — re-run `maj` (bridge + vp + inst + market).
- Cost: minutes to ~1 hour for a full Eagle rebuild.

### Irrecoverable (still needs separate backup)

- **`/Volumes/Sam128/TC_Sam128/*.scid`** — the 16 historical contracts (~13 GB). Sierra has purged them; if Sam128 fails, the 2.5 years of historical NQ + 2.5 years of historical GC are **gone**. Too large for GitHub free tier (100 MB file limit; the 1.4 GB NQ files in particular).
- `Pulse/Data/Ticks_Parquet_Training/` — 1.7 GB on Mac internal. Derived from the .scid, so reproducible IF the .scid survive. Also too large for GitHub.

### Backup recommendation (.scid archive only — everything else is already replicated)

- `TC_Sam128/*.scid`: replicate the 13 GB to a second physical disk (cloud, external drive, or partner Mac).
- Cadence: monthly snapshot sufficient — only the active 6 contracts grow via Pulse `sync`, and the 16 historical contracts never change.
- Recovery test: occasionally try a `--full` rebuild from a restored backup to confirm the archive is readable.

---

## 9. Change Log

- 2026-05-12 (late afternoon) — Created. Captures the architecture that emerged from the Path B refactor: Eagle's find_scid() order, parquet location at TC_Sam128, Pulse-owns-sync convention, disaster recovery priorities. Absorbed material from TC_Main § 4 (Active Datasets) and the Sam128 layout paragraph; TC_Main will retain a pointer to this file.
