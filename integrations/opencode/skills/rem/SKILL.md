---
name: rem
description: >
  Quick-action memory command. Saves findings to or recalls information from
  the hyperkb knowledge base using natural language. Infers whether to search
  or save from phrasing and conversation context. Use when the user types /rem.
compatibility: opencode
metadata:
  audience: all
  workflow: knowledge-management
---

# /rem — Quick Memory Action

You have access to 10 `mcp__hyperkb__hkb_*` MCP tools. Use them directly as tool
calls — do not use bash or shell commands.

## Step 1: Determine Intent

Read the user's arguments and classify:

| Signal | Intent |
|--------|--------|
| Question words (what, when, how, where, who, did, do, is, are, was, were, which, any) | **SEARCH** |
| "recall", "look up", "find", "get", "check", "show" | **SEARCH** |
| "remember", "save", "store", "record", "note", "log", "add" | **SAVE** |
| Declarative statement of fact ("the API uses JWT", "port 8443 works") | **SAVE** |
| "technique", "workaround", "trick", "how to", "the fix was", pattern of problem+solution | **SAVE** as @type: skill |
| Mixed ("remember what we found about X") | **BOTH** — search first, then save results |
| "save everything", "save all findings", "bulk save" | **BULK** — scan conversation |
| Empty / no arguments | **INFER** from conversation context |

## Step 2a: SEARCH Flow

1. Extract 2-4 **keywords** from the query (not the full sentence).
2. Map timeframe hints: "recently" → `after="3d"`, "today" → `after="1d"`, "this week" → `after="7d"`.
3. Call `mcp__hyperkb__hkb_search` with keywords, mode `hybrid`, top 10.
4. If results are sparse (0-1 hits), broaden: fewer keywords, remove domain, or
   call `mcp__hyperkb__hkb_show` (no name) to find files, then `mcp__hyperkb__hkb_show` with `name` and `last=3`.
5. Present results conversationally with file:epoch for traceability.

**Autonomy: Search immediately. No confirmation needed.**

## Step 2b: SAVE Flow

1. **Determine content:** Direct fact → use it. Conversation reference → distill 2-5 sentences. Procedure → `@type: skill` with Problem/Solution/Context.
2. **Write self-contained entry** with what, why, and enough context for months later.
3. **Find target file:** `mcp__hyperkb__hkb_show` (no name) → pick obvious match → `mcp__hyperkb__hkb_add` with `to`.
4. **Handle response:**
   - `"ok"` → done, report file:epoch.
   - `"no_match"` → `mcp__hyperkb__hkb_add` with `create_file=True`, then retry with `to=`.
   - `"low_confidence"` → pick best candidate, retry with `to=`.
   - **Never re-call without `to=`** — same result every time.

**Autonomy:** Explicit fact → save immediately. Synthesized → show draft, ask first.

## Step 2c: BOTH Flow

1. SEARCH first → show results → ask to save → SAVE flow.

## Step 2d: INFER Flow (No Arguments)

**Save signals** — propose saving:
- Bug debugged (root cause found)
- Decision made (architecture, approach)
- Configuration discovered (non-obvious settings)
- Workaround used (procedural fix)
- Milestone reached (feature complete, migration done)

**Skip signals** — show memory status instead:
- Routine edits, work in progress, info already in codebase

For skip: `mcp__hyperkb__hkb_show` with `sort="recent"`, report file count + recent files.
For save: show draft entry and target, ask confirmation.

## Step 2e: BULK Flow

1. Scan conversation for distinct, save-worthy findings.
2. Group by topic — one entry per finding.
3. Show numbered list: `1. [target-file] summary`.
4. Save on confirmation. Never combine unrelated findings.

## Dedup Strategy

Before saving, search 2-3 key terms:
- **Same fact + conclusion** → skip, cite existing file:epoch.
- **Same topic + newer info** → save new, mark old `superseded`.
- **Related but distinct** → save normally, add `[[links]]`.

## Rules

1. **Be fast.** Minimize tool calls. If you know the file, add directly.
2. **Never save trivial content.** No "updated the README", no routine refactors.
3. **Write good entries.** Self-contained, 2-5 sentences, one topic. Include *why*.
4. **Don't double-save.** Quick search first if info might already exist.
5. **Keep responses concise.** Brief summary for searches, one-line for saves.
6. **No `>>>` or `<<<` in content.** Entry delimiters — will corrupt the file.
7. **Always use @type: skill for procedural knowledge.**
