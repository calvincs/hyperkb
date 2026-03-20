# hyperkb — Persistent Knowledge Base

You have a persistent knowledge base available via the `hyperkb` MCP server.
It stores structured entries across markdown files with hybrid search. Use it
to recall past findings, decisions, configurations, and context across sessions.

## Session Start (REQUIRED)

At the START of every session, call `mcp__hyperkb__hkb_session` (action="briefing")
to see what knowledge files exist, recent activity, and open tasks.

## When to Use

- **Before starting work on any topic**: search the KB first — past sessions may
  have relevant findings that save time and avoid repeating mistakes.
- **When the user asks about something you don't know**: search with
  `mcp__hyperkb__hkb_search` using 2-4 keywords (NOT full sentences).
- **When the user says "remember", "save", "note", "store"**: add an entry with
  `mcp__hyperkb__hkb_add`.
- **When you discover something worth preserving**: proactively save key findings,
  bug root causes, architecture decisions, hard-won configurations, and procedural
  skills (workarounds, techniques, how-to steps).

## Quick Reference (10 tools)

| Task | Tool | Key Params |
|------|------|------------|
| Session overview | `mcp__hyperkb__hkb_session` | `action="briefing"`, `focus`, `view` |
| Search entries | `mcp__hyperkb__hkb_search` | `query` (keywords), `mode`, `domain` |
| List files | `mcp__hyperkb__hkb_show` | (no name), `domain`, `sort` |
| Read a file | `mcp__hyperkb__hkb_show` | `name`, `last`, `compact` |
| Add entry | `mcp__hyperkb__hkb_add` | `content`, `to` (target file) |
| Create file | `mcp__hyperkb__hkb_add` | `create_file=True`, `to`, `description`, `keywords` |
| Recent timeline | `mcp__hyperkb__hkb_search` | `mode="recent"`, `top`, `after` |
| Update entry | `mcp__hyperkb__hkb_update` | `file`, `epoch`, `new_content`, `set_status` |
| Tasks | `mcp__hyperkb__hkb_task` | `action` (create/show/update/list) |
| Token-budgeted context | `mcp__hyperkb__hkb_context` | `topic`, `max_tokens`, `mode` |
| Health checks | `mcp__hyperkb__hkb_health` | `checks` (all/quick/links/sync/quality), `fix` |
| Sync with S3 | `mcp__hyperkb__hkb_sync` | `action` (both/push/pull/status), `dry_run` |
| Named views | `mcp__hyperkb__hkb_view` | `action` (set/list), `name`, `files` |

For the full skill reference, load the `hyperkb` skill. For quick save/recall,
load the `rem` skill.

## Troubleshooting

If MCP tool calls return empty results (0 files, 0 entries) but you suspect
data should exist:

1. **Check scope**: Files live at `~/.hkb/storage/`.
2. **Try reindex**: Call `mcp__hyperkb__hkb_health` with `action="reindex"` to rebuild the search index.
3. **Never fall back to reading raw files with bash** — fix the MCP connection instead.
4. **Do NOT look at `~/.claude/` config** — that is for Claude Code, not OpenCode.
   OpenCode config is at `~/.config/opencode/opencode.json`.
