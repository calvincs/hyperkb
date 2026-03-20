---
name: rem
description: >
  Quick-action memory command. Saves findings to or recalls information from
  the hyperkb knowledge base using natural language. Infers whether to search
  or save from phrasing and conversation context. Use when the user types /rem.
---

# /rem — Quick Memory Action

You have access to 10 `hkb_*` MCP tools (hkb_search, hkb_show, hkb_add, hkb_update,
hkb_task, hkb_sync, hkb_session, hkb_context, hkb_view, hkb_health). Use them directly — no CLI needed.

## Step 1: Determine Intent

Read `$ARGUMENTS` and classify:

| Signal | Intent |
|--------|--------|
| Question words (what, when, how, where, who, did, do, is, are, was, were, which, any) | **SEARCH** |
| "recall", "look up", "find", "get", "check", "show" | **SEARCH** |
| "remember", "save", "store", "record", "note", "log", "add" | **SAVE** |
| Declarative statement of fact ("the API uses JWT", "port 8443 works") | **SAVE** |
| "technique", "workaround", "trick", "how to", "the fix was", pattern of problem+solution | **SAVE** as @type: skill |
| Mixed ("remember what we found about X") | **BOTH** — search first, then save results |
| "save everything", "save all findings", "bulk save" | **BULK** — scan conversation for findings |
| Empty / no arguments | **INFER** from conversation context |

## Step 2a: SEARCH Flow

1. Extract 2-4 **keywords** from the query (not the full sentence).
2. Map timeframe hints to `after`/`before` params:
   - "recently" / "last few days" → `after="3d"`
   - "today" → `after="1d"`, "this week" → `after="7d"`, "last month" → `after="30d"`
3. Call `hkb_search` with keywords, mode `hybrid`, top 10.
   - If a domain is obvious from context, pass `domain`.
4. If results are sparse (0-1 hits), broaden:
   - Try fewer keywords, remove domain qualifier.
   - Try `hkb_show()` to find relevant files, then `hkb_show(name="file", last=3)` on matches.
5. Present results conversationally — summarize, don't dump raw JSON.
   - Include file:epoch for traceability (e.g. "Found in security.auth:1740130800").
   - If nothing found, say so clearly.

**Autonomy: Search immediately. No confirmation needed.**

## Step 2b: SAVE Flow

1. **Determine content:**
   - If `$ARGUMENTS` contains the fact directly → use it.
   - If referencing conversation work ("save what we found") → distill into a 2-5 sentence entry.
   - If it describes a procedure/workaround → use `@type: skill` with Problem/Solution/Context.
2. **Write a self-contained entry** — include what, why, and enough context to be useful months later.
   - Use `[[file.name]]` wiki-links for cross-references (only for real KB files).
   - One focused topic per entry. Split multi-topic content into separate adds.
3. **Find the target file:**
   - Call `hkb_show()` to list files, pick the obvious match.
   - If sure → `hkb_add(content="...", to="file.name")`.
   - If unsure → `hkb_add(content="...")` to auto-route.
4. **Handle the response:**
   - `"ok"` → done. Report file name and epoch.
   - `"no_match"` → create file first: `hkb_add(create_file=True, to="name", description="...", keywords=[...])`, then retry with `to=`.
   - `"low_confidence"` → pick best candidate from the list and retry with `to=`, or create new file.
   - **Never re-call without `to=`** — same result every time.
5. Confirm the save. Report file:epoch.

**Autonomy:**
- Explicit fact ("remember that X uses Y") → save immediately, no confirmation.
- Synthesized from conversation → show draft entry and target file, ask before saving.

## Step 2c: BOTH Flow (Search then Save)

1. Run the SEARCH flow first.
2. Show results to the user.
3. Ask if they want to save the findings, then run SAVE flow.

## Step 2d: INFER Flow (No Arguments)

Look at the conversation for save-worthy signals:

**Save signals** — propose saving:
- Bug debugged (root cause identified)
- Decision made (architecture, tool choice, approach)
- Configuration discovered (non-obvious settings, env vars)
- Workaround used (procedural fix for a known issue)
- Milestone reached (feature complete, migration done)

**Skip signals** — show memory status instead:
- Routine edits (rename, formatting, linting)
- Work in progress (not yet concluded)
- Information already in the codebase (README, comments, config files)

For skip: call `hkb_show(sort="recent")` and report file count + recently updated files.
For save: show draft entry and target file, ask for confirmation.

## Step 2e: BULK Flow (Save Everything)

1. Scan the conversation for distinct, save-worthy findings.
2. Group by topic — one entry per distinct finding.
3. Show a numbered list: `1. [target-file] summary of entry`.
4. Save on user confirmation. Never combine unrelated findings into one entry.

## Dedup Strategy

Before saving, search 2-3 key terms to check for existing entries:

- **Same fact, same conclusion** → skip. Tell the user it already exists (cite file:epoch).
- **Same topic, newer/different info** → save new entry, then `hkb_update(file, epoch, set_status="superseded")` on the old one.
- **Related but distinct** → save normally, add `[[links]]` to related entries.

## Rules

1. **Be fast.** Minimize tool calls. If you know the file, add directly.
2. **Never save trivial content.** No "updated the README", no routine refactors, no raw tracebacks.
3. **Write good entries.** Self-contained, 2-5 sentences, one topic. Include *why*, not just *what*.
4. **Don't double-save.** Quick `hkb_search` first if you suspect info already exists.
5. **Keep responses concise.** Search results get a brief summary. Saves get a one-line confirmation.
6. **No `>>>` or `<<<` in content.** These are entry delimiters and will corrupt the file.
7. **Always use @type: skill for procedural knowledge.** Workarounds, techniques, how-to steps.
