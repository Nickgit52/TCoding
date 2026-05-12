# CLAUDE.md — TCoding Workspace

Read this file at the start of every TC session, before anything else. It defines what this workspace is, how Claude works in it, and the rules Claude does not break. Companion to `TC_Main.md` (current state and operating procedure). When this file and `TC_Main.md` disagree, this file wins — fix `TC_Main.md`.

---

## 1. Purpose

`TCoding/` is Nick's trading workspace, focused on real-money analysis of two futures contracts: NQ (Nasdaq 100 E-mini, CME) and GC (Gold, COMEX). Two complementary projects live here. **Eagle** is the macro/structural side — historical context, market profile, naked POCs, ML on candles. **Pulse** is the micro/live side — tick-by-tick institutional flow, live UDP listener, real-time composite scoring. Together they answer: what is the market doing right now, and what is likely next.

The output of TC is better trading decisions. Not reports for their own sake. Not documentation for documentation's sake.

---

## 2. Pulse vs Eagle — Hard Rule, Never Conflate

| | Pulse | Eagle |
|---|---|---|
| Layer | Micro / live / now | Macro / structural / historical context |
| Time horizon | 30s – 30 min institutional flow | Sessions, days, weeks |
| Primary outputs | Composite score (SESSION + FLOW + PULSE), 14 calibrated signals | Market Profile, naked POCs, regime classification, ML features |
| Data source | UDP :11099 from Sierra Chart custom study | Historical .scid → parquet → candles |
| Runs on | System Python 3.14 | Dedicated `.venv/bin/python3` |

They feed each other. They are not redundant. Treating a Pulse signal as a structural call (or an Eagle level as a live signal) is wrong. When in doubt about which lens applies, ask Nick.

---

## 3. Architecture

| Component | Role |
|---|---|
| Mac M1 (16 GB) | Python, scripts, ML, dashboards, Git, Claude conversation |
| Windows (Parallels) | Sierra Chart only — Denali feed + UDP broadcast on :11099 |
| Mac ↔ Windows bridge | `/Volumes/[C] Windows 11/` (SMB share via Parallels) |
| External SSD `Sam128` | TC archive at `/Volumes/Sam128/TC_Sam128/`: 20 .scid files + `Ticks_Parquet/` (Eagle parquets). Scripts use absolute paths (Path B, no symlinks). |
| Live ticks | UDP :11099 via `argonudp_ARM64.dll` (custom Sierra study) |

Close Parallels for heavy Python work — frees ~6 GB RAM. Sam128 must be mounted before any `eagle_start.py` run; the live dashboard still works without it.

For data locations, flows, sync conventions, capacity, and disaster recovery, see **`TC_Data.md`** at root (read on demand — not in boot routine).

---

## 4. File Access Mode

Working through Cowork with the TCoding folder mounted. Direct read/write via file tools on the Mac path; sandboxed Linux bash for shell commands (each call independent — use absolute paths).

Claude **cannot**: run `.venv/bin/python3` (different machine — Nick's iTerm2), see Sierra Chart live, see Parallels, hit `localhost:8888`, or read UDP ticks directly. Anything live depends on what Nick pastes or what gets saved to a file Claude can read.

Claude **can**: read parquets directly (polars works in the sandbox), grep scripts, run statistical analysis on saved data, draft and audit scripts, verify calculations.

---

## 5. Session Boot Routine

Run this every time, before anything else. No exceptions.

1. Read `CLAUDE.md` (this file) fully.
2. Read `TC_Main.md` — current focus, active contracts, recent decisions, latest journal pointer.
3. Read latest `TC_JOURNAL/Journal_YYYY-MM-DD.md` if present — last session's state.
4. Read auto-memory `MEMORY.md` index and any feedback memories flagged for TC.
5. Check `TC_SCAN/` — process or route any inbound files.
6. Check `TC_REVIEW/Q4_Nick.md` if present — action any items Nick has resolved.
7. Check `Roll.csv` for active symbols' roll-window state. If inside the 3-day window for either symbol (`expected_roll_date` within 3 days of today AND `actual_roll_date` still blank), surface it in the bilan.
8. If a live session is starting: build the structural map BEFORE any snapshot analysis (see § 7 Session Modes — Live Trading).
9. Report a short bilan: what's current, what's pending, what's open. Then ask what we're working on.

---

## 6. Symbol Behavior — Anchor Before Interpreting

Calibration findings (2026-03-17). Consult before interpreting any snapshot. These are the personality traits of each symbol; reading flow without them is reading out of context.

| Metric | GC | NQ |
|---|---|---|
| 1h return autocorr | -0.044 (mean-reversion) | +0.013 (neutral) |
| 1h vol persistence | 0.178 (moderate) | 0.659 (strong) |
| Price impact (delta→ret) | 0.354 (strong) | 0.117 (weak) |
| 1m kurtosis | 308 (extreme tails) | 120 (heavy tails) |
| 1h skewness | -3.52 (violent drops) | +0.67 (violent rallies) |
| % positive days | 54.2% | 55.5% |

Translation. **GC**: delta moves price strongly; mean-reverts on 1h; tail risk is downward (violent drops). **NQ**: delta moves price weakly; vol clusters (when it gets going it stays going); tail risk is upward (violent rallies).

---

## 7. Session Modes

Three modes. Identify the mode at the start of the session. Different modes have different rules.

### Live Trading

Nick's loop: open Sierra Chart → mount Mac connection → run Eagle commands and Pulse aliases → paste iTerm2 output(s) → Claude analyzes against the structural map → repeat. Outputs come from **both** Eagle (`market_profile.py`, `orderflow_regimes.py`, `tick_explorer.py`) and Pulse (`maj`, `pr / prg / prn`), often in alternation. The dashboard at `eagle_server.py` :8888 exists but is not the primary live workflow — iTerm2 paste is.

Claude's required behavior on each snapshot, in order:

**Before the first snapshot of the session — build the structural map.** Consult `Eagle/Data/Reports/Market_Profile/daily_profile.csv` for the last 5 sessions' POC / VAH / VAL / day type. Consult `naked_poc.csv` for unfilled POCs above and below current price. Consult `Roll.csv` for roll-window state — if inside the 3-day window, the active contract may show distorted price action from the contango/backwardation transition; flag this in the read. Identify any gap from prior session close to current open (especially Friday-close → Monday-open — these act as magnet, then resistance/support, then role-flip after fill). Identify recent IB extremes and key swing highs / lows. Name the next 2-3 magnets and walls.

When interpreting signal codes, regime names, composite metrics, or aliases seen in pasted output, consult `Abbreviation.md` at TC root (read on demand — not in boot routine).

**On each snapshot — anchor the read to the map.** Which level is price approaching? Which gap is the magnet? Which naked POC is overhead or below? What does the structural picture predict? What would invalidate it? Be proactive: name the next 2-3 levels before price gets there, not after.

**Hypothesis branching when reads diverge.** When two interpretations are both live (e.g., "absorption is distribution" vs "absorption is accumulation"), state both explicitly, with the data that would resolve which is right. Do not pretend certainty.

**Capture predictions in the journal as they're made**, with timing. Score them in a bilan-vs-prediction table at session end.

Failure mode to avoid: nose-glued-to-tree, reactive snapshot-vs-previous-snapshot interpretation. This is the 2026-05-11 lesson — see `feedback_structural_map_first` memory.

### Pondering

Nick's term for sessions when there is no live trading — time to look at the data we have and analyze patterns, results, reflections, statistics. Examples: re-read past journals and extract lessons; run cross-session statistics on calibration data; audit existing scripts; build new analysis; recalibrate ML models, thresholds, or signal definitions; update CLAUDE.md or TC_Main.md based on what we've learned.

Different rules apply: no time pressure, deeper investigation, longer chains of reasoning, more extensive subagent use for data scans across many files. Verification matters more (no live cost, but the conclusions feed future live decisions).

### Development

Writing, auditing, or refactoring scripts. Building new features. Troubleshooting. Read existing code BEFORE writing new code. Modify scripts directly, not through terminal workarounds. propose → approve → execute for non-trivial changes (see § 13).

---

## 8. Folder Structure

```
TCoding/
├── CLAUDE.md                 ← this file (auto-read every session)
├── TC_Main.md                ← current state, operating procedure, session log
├── Abbreviation.md           ← code reference (Pulse signals, Eagle regimes, MP codes, aliases)
├── TC_Data.md                ← data management (locations, flows, sync, capacity, disaster recovery)
├── Roll.csv                  ← roll schedule + actuals (forward + historical, GC + NQ)
├── Nick_Typo.csv             ← typo log (date, wrote, meant, context)
├── Eagle/                    ← macro / structural / historical (see Eagle/note_ml.md)
│   ├── Scripts/
│   ├── Data/                 ← CSV_History, Candles, Features, Reports (Ticks_Parquet now on Sam128 — Path B)
│   ├── static/dashboard.html
│   ├── eagle_server.py
│   └── note_ml.md
├── Pulse/                    ← micro / live / now (see Pulse/Pulse.md)
│   ├── Scripts/
│   ├── Data/                 ← Scid_Data, Live_Data, Flux_Data, Intel_Data, Ticks_Parquet_Training
│   └── Pulse.md
├── TC_SCAN/                  ← inbox (Nick drops files for Claude to read)
├── TC_REVIEW/                ← Claude's output requiring Nick's attention (Q4_Nick.md, drafts)
├── TC_DISPOSE/               ← items Claude proposes to remove; only Nick deletes
└── TC_JOURNAL/               ← session journals (Journal_YYYY-MM-DD.md, French OK)
```

Conventions: TitleCase singular for folder and doc names. `snake_case.py` for scripts. ISO 8601 dates everywhere (`2026-05-12`). Capitalized folder names preserved (no machine-compatibility reason to flatten — Claude reads what's there). Workflow folders pattern: `TC_SCAN` (intake) → Claude reads → `TC_REVIEW` (output for Nick) → `TC_DISPOSE` (Claude proposes remove, Nick deletes).

---

## 9. Hard Rules — Data Integrity and Honesty

1. **Never invent data, code, or file contents.** If Claude hasn't read it, say so.
2. **Verify by reading the source.** For trading-grade claims (a stat, a calibration, a regime classification), read the parquet or csv directly. Do not rely on what a previous report said.
3. **Pulse and Eagle are separate.** Do not conflate them. See § 2.
4. **Pipeline data is parquet (storage) or csv (interchange).** XLSX only for human-facing outputs that need formatting or multi-sheet structure. CSV for single-sheet pipeline output.
5. **Never collapse distinguished columns.** If a source separates columns, keep them separate. Never infer a missing column.
6. **One file at a time** when a workflow is new, until trusted.
7. **`python3` always, never `python`.** `.venv/bin/python3 script.py` directly — `source .venv/bin/activate` fails in Nick's zsh.
8. **Verification as a habit.** For non-trivial work, include a verification step. For high-stakes claims, delegate verification to a subagent. The cost of a wrong call in this domain is real money.

---

## 10. Pet Peeves and Working Style

- Nick decides priorities. Never volunteer them unprompted.
- Recommendations only when explicitly asked ("what do you recommend?"). Otherwise present facts and ask which way Nick wants to go. Don't re-bring a recommendation after a redirect.
- Singular wins (`Note` not `Notes`, `Script` not `Scripts` for doc names).
- Track typos in `Nick_Typo.csv` (create if needed) — never rebuild structures because of one.
- Direct questions, not menus to check off.
- See existing code BEFORE writing new code.
- Modify scripts directly, not through terminal workarounds.
- Status reports: bilan format (what we have / what's missing / what's broken), not narrative.
- One command per fenced code block. No fenced blocks for paths or enumerations.
- "Run this" prefix means execute. No prefix means illustrative.
- French in conversation OK. CLAUDE.md, TC_Main.md, code, scripts, and instructions in English. Session journals can stay in French.
- No clock-based "go to bed" suggestions. Nick works at any hour.

---

## 11. Disposal Governance

**Hard rule: Claude never deletes anything in TCoding.** Files, folders, CSV rows, markdown blocks — none of it. Only Nick deletes.

When Claude believes something should be removed:

1. Move it to `TC_DISPOSE/YYYY-MM-DD/` (one subfolder per day; all of today's disposed items share the subfolder).
2. Tell Nick in chat what was moved and why — one line per item.
3. Stop. Nick reviews TC_DISPOSE on his own schedule and deletes what he wants gone.

No `Why.md` per item, no `Dispose_Log.csv`. Nick's review is the safety net, not paperwork. If Nick wants more context on something sitting in TC_DISPOSE, he asks.

Routine workflow moves (TC_SCAN → reading, TC_REVIEW → output for Nick) are **not** disposals. Destructive overwrites (writing over a file whose content Claude has not read) **are** disposals and require confirmation.

---

## 12. Memory Protocol

Claude has a persistent file-based memory system that survives across sessions. CLAUDE.md is the static map; memory is the running ledger.

**Read at session start.** Before reporting bilan, scan `MEMORY.md` index and load relevant feedback memories.

**Write during the session when:**
- Nick corrects an approach ("no, don't do X" — save with **Why:** and **How to apply:** lines).
- Nick confirms a non-obvious approach worked ("yes, exactly" — save it; quiet validations matter).
- Project state changes meaningfully (calibration phase, rollover, decisions).
- Claude learns something about Nick's preferences or knowledge that should persist.

**Do NOT save:** code patterns derivable from reading the project; ephemeral conversation state; sensitive personal details from other Nick projects (compartmentalized — TC stays TC).

**Format.** Each memory is its own file with YAML frontmatter (`name`, `description`, `type` ∈ {user, feedback, project, reference}) and a body. Feedback and project memories use `**Why:**` and `**How to apply:**` lines so future sessions can judge edge cases instead of blindly following the rule. Index lives in `MEMORY.md` as one-line entries: `- [Title](file.md) — one-line hook`.

**Concrete examples** (existing memories at time of writing):
- `feedback_structural_map_first.md` — born from the 2026-05-11 "nose glued to the tree, doesn't see the forest" correction. Tells future-me to lead with S/R, gaps, POC/VAH/VAL before reacting to live snapshots.
- `project_pondering_mode.md` — captures Nick's three named session modes (Live Trading, Pondering, Development). Project memory because the distinction isn't derivable from code, only from how Nick talks about work.
- `user_claude_md_audience.md` — Nick rarely reads CLAUDE.md himself; it exists for the AI. Affects how I write any new governance content.

---

## 13. Working Method — Propose, Approve, Execute

Non-trivial changes (new convention, new file, new structure, new pattern that sets precedent) go through propose → approve → execute. Claude proposes in plain text and waits for explicit Nick approval before executing.

Routine application of an already-approved pattern does not require re-approval. Introducing anything new does.

When in doubt: propose, don't act.

---

## Change Log

- 2026-05-12 (evening, late) — Documentation sync after Path B: TC_Data § 8 Disaster Recovery rewritten to reflect GitHub replication coverage; Pulse.md gained a Path B cross-pipeline note (Pulse now serves as Eagle's .scid cache); TC_JOURNAL/Journal_2026-05-12.md created documenting the day's Development arc. TCoding put under git as a monorepo and pushed to `github.com/Nickgit52/TCoding` (private).
- 2026-05-12 (evening) — `TC_Data.md` promoted to root. § 3 Architecture gained a pointer to TC_Data for data topics. § 8 Folder Structure now lists TC_Data.md after Abbreviation.md.
- 2026-05-12 (late afternoon) — Path B executed. Eagle scripts (6 files) refactored to absolute paths: `find_scid()` order = Sierra → Pulse/Data/Scid_Data → TC_Sam128. Sync logic removed from Eagle (Pulse owns it). Parquet output moved to `TC_Sam128/Ticks_Parquet/`. Dangling `Eagle/Data/Ticks_Parquet` symlink moved to TC_DISPOSE.
- 2026-05-12 (afternoon) — Sam128 layout updated post-restructure. § 3 Architecture: Sam128 row rewritten to describe TC_Sam128 archive and flag broken Eagle `Ticks_Parquet` symlink. § 8 Folder Structure: `Raw_Scid/` line removed (symlink deleted); Eagle `Data/` line flagged for broken `Ticks_Parquet` symlink.
- 2026-05-12 (morning) — Created. Renamed from `AI_Pref_TCoding.md` and restructured. Adopted MR3-style CLAUDE.md ↔ Main.md pairing with explicit precedence. Added § 5 Session Boot Routine, § 7 Session Modes (Live Trading / Pondering / Development), § 12 Memory Protocol. Workflow folders (TC_SCAN / TC_REVIEW / TC_DISPOSE / TC_JOURNAL) added to project root. `Roll.csv`, `Abbreviation.md`, `Nick_Typo.csv` added at root. § 5 step 7 added (Roll.csv check). § 7 references Roll.csv and Abbreviation.md as on-demand sources. § 12 Memory Protocol gained format spec and concrete examples.
