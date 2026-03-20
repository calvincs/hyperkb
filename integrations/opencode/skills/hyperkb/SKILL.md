---
name: hyperkb
description: >
  Persistent knowledge base for storing and retrieving structured knowledge
  across sessions. Use this skill whenever the user wants to save, store,
  remember, recall, look up, or search for knowledge, findings, notes, or
  any persistent information. Also use when the user mentions "hkb", asks to
  "add this to my notes", or wants to cross-reference stored information.
  Trigger on: "remember this", "save this", "look up", "what do I know about",
  "add to knowledge base", "find my notes on", "store this finding",
  "hkb search", "hkb add".
compatibility: opencode
metadata:
  audience: all
  workflow: knowledge-management
---

# hyperkb Skill

## Mental Model

Files are named topics (`domain.topic.subtopic`, 2-4 dot-separated segments). Each file holds timestamped entries delimited by `>>> epoch` / `<<<` markers. Entries have structured metadata (`@type`, `@status`, `@weight`, `@tags`) parsed from lines at the start of content. Wiki-links `[[file.name]]` cross-reference between files. Markdown files in `~/.hkb/storage/` are source of truth; SQLite is a rebuildable index.

## Which Tool Do I Use?

### I need to FIND something

- **Know the file** → `mcp__hyperkb__hkb_show` with `name`
- **Don't know the file** → `mcp__hyperkb__hkb_search` with `query` (2-4 keywords, not sentences)
- **Exact string/regex** → `mcp__hyperkb__hkb_search` with `mode="rg"`
- **List all files** → `mcp__hyperkb__hkb_show` (no name)
- **Cross-file timeline** → `mcp__hyperkb__hkb_search` with `mode="recent"`
- **Token-budgeted context** → `mcp__hyperkb__hkb_context` with `topic`
- **File suggestions** → `mcp__hyperkb__hkb_context` with `mode="suggest"`
- **Link graph** → `mcp__hyperkb__hkb_show` with `name` and `links=True`

### I need to SAVE something

- **Know the file** → `mcp__hyperkb__hkb_add` with `content` and `to`
- **Unsure** → `mcp__hyperkb__hkb_add` with `content` only (auto-routes) — handle response
- **Preview routing** → `mcp__hyperkb__hkb_search` with `mode="check"` and `query`
- **New file needed** → `mcp__hyperkb__hkb_add` with `create_file=True`, `to`, `description`, `keywords`

### I need to UPDATE something

- **Amend entry** → `mcp__hyperkb__hkb_update` with `file`, `epoch`, `new_content`/`set_status`/`add_tags`
- **Archive entry** → `mcp__hyperkb__hkb_update` with `action="archive"`, `file`, `epoch`
- **Bulk changes** → `mcp__hyperkb__hkb_update` with `action="batch"`, `updates=[...]`

### I need to ORIENT myself

- **Session start** → `mcp__hyperkb__hkb_session` with `action="briefing"`
- **Topic focus** → add `focus="topic"` or `view="view-name"` to briefing
- **What changed** → `mcp__hyperkb__hkb_session` with `action="review"`, `after="1d"`
- **Bias searches** → `mcp__hyperkb__hkb_session` with `action="anchor"`, `topics="auth, security"` (1.5x boost, not a filter)

### I need to MANAGE

- **Tasks** → `mcp__hyperkb__hkb_task` with `action` (create/show/update/list)
- **Health** → `mcp__hyperkb__hkb_health` with `checks="all"`, `fix=True`
- **Reindex** → `mcp__hyperkb__hkb_health` with `action="reindex"`
- **Sync** → `mcp__hyperkb__hkb_sync` with `action="both"`
- **Views** → `mcp__hyperkb__hkb_view` with `action="set"`, `name`, `files`

## Handling hkb_add Responses

| Response | Meaning | Next Step |
|----------|---------|-----------|
| `"ok"` | Saved. May include `broken_links` warning. | Done. Fix broken `[[links]]` if warned. |
| `"no_match"` | No file matched. | Create file first with `create_file=True`, then retry with `to=`. |
| `"low_confidence"` | Candidates listed, none confident. | Pick one and retry with `to=`, or create new file. |

**Never re-call without `to=`** — auto-routing returns the same result every time.

## Writing Good Entries

**Metadata format:** `@key: value` lines at the START of content, parsed into DB columns:
```
@type: finding
@weight: high
@tags: security, auth
Connection pool exhaustion at 50+ concurrent requests. Root cause: pool size 10, no checkout timeout.
```

**Types:** `note` (default), `finding` (investigation results), `decision` (exempt from staleness), `task`, `milestone`, `skill` (Problem/Solution/Context format).

**@type: skill example:**
```
@type: skill
@tags: docker
Problem: Container can't reach host services on macOS.
Solution: Use host.docker.internal instead of localhost.
Context: macOS/Windows Docker Desktop only. Linux uses --network=host.
```

**Good entry:** Self-contained, includes why, useful months later.
**Bad entry:** "Fixed the search bug." (no context, not useful later)

**@weight:** `high` for critical info (~5% of entries), `low` for temporary workarounds.

**Constraints:** No `>>>` or `<<<` in content. Max 1 MiB. One topic per entry.

## File Naming

`domain.topic[.subtopic].md` — 2-4 lowercase dot-separated segments. Use project name as first segment.

## Troubleshooting

**Empty results:** Call `mcp__hyperkb__hkb_health` with `action="reindex"`. Check that `--path` wasn't passed to hkb-mcp.
**Bad search results:** Use keywords not sentences. Try `mode="rg"`. Use `domain` to narrow.
**OpenCode config:** `~/.config/opencode/opencode.json` — NOT `~/.claude/`.
