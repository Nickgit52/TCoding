# Q4_Nick.md — Open Questions for Nicolas

Park place for items needing operator review. Each question gets a number, a date, narrative, the open ask, and a suggested resolution. Resolved questions stay in this file with a `RESOLVED` tag and the resolution noted.

---

## Q1 — Approve renamed CLAUDE.md and TC_Main.md (2026-05-12) — RESOLVED 2026-05-12

Two drafts placed in TC_REVIEW for review. Nick approved the CLAUDE draft as-is and the TC_Main draft with three corrections:

1. Disposal Governance simplified — no `Why.md` per item, no `Dispose_Log.csv`. Nick reviews TC_DISPOSE on his own schedule and deletes himself; Claude tells him in chat one line per disposed file.
2. Active Contracts (TC_Main § 1) rewritten — three contracts kept accessible (front + next + last-for-3-weeks because expired contracts keep useful updates for pondering). Two-phase roll detection: once-daily check outside the 3-day roll window, active monitoring inside. Crossover triggers a notification to Nick.
3. Project Status / Eagle (TC_Main § 3) corrected — the dashboard at `eagle_server.py` :8888 exists but is **not** the primary live workflow. Actual workflow is iTerm2 reports from Eagle and Pulse, copy-pasted to Claude in alternation.

Three new memories saved as part of this resolution: `project_pondering_mode` (the three session modes), `project_exchange_workflow` (iTerm2 from both Eagle and Pulse, not the dashboard), and an update to `project_tc_workflow_folders` capturing the lightweight disposal preference.

**Resolution executed:** `AI_Pref_TCoding.md` and `TCoding.md` moved to `TC_DISPOSE/2026-05-12/`. `CLAUDE_DRAFT.md` promoted to `/CLAUDE.md`. `TC_Main_DRAFT.md` promoted to `/TC_Main.md`. The five MR3 reference files moved from `TC_SCAN/` to `TC_DISPOSE/2026-05-12/`. Project root grep confirmed no broken references — remaining mentions of the old filenames are in change-log entries (intentional historical record).
