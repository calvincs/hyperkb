---
name: hyperkb
description: >
  Hyperconnected knowledge base for storing and retrieving structured knowledge
  across markdown files with hybrid search. Use this skill whenever the user wants
  to save, store, remember, recall, look up, or search for knowledge, findings,
  notes, observations, configurations, or any persistent information. Also use when
  the user references their knowledge base, mentions "hkb", asks to "add this to my
  notes", or wants to cross-reference stored information. Trigger on: "remember this",
  "save this", "look up", "what do I know about", "add to knowledge base", "find my
  notes on", "store this finding", "store this skill", "how did we fix",
  "hkb search", "hkb add".
---

# hyperkb Skill

## Mental Model

Files are named topics (`domain.topic.subtopic`, 2-4 dot-separated segments). Each file holds timestamped entries delimited by `>>> epoch` / `<<<` markers. Entries have structured metadata (`@type`, `@status`, `@weight`, `@tags`) parsed from lines at the start of content. Wiki-links `[[file.name]]` cross-reference between files. Markdown files in `~/.hkb/storage/` are source of truth; SQLite is a rebuildable index.

## Which Tool Do I Use?

### I need to FIND something

- **Know the file name** → `hkb_show(name="file.name")` to read it
- **Don't know the file** → `hkb_search(query="2-4 keywords")` — use keywords, not sentences
- **Need exact string or regex** → `hkb_search(mode="rg", query="exact phrase")`
- **Want a list of all files** → `hkb_show()` (no name)
- **Want cross-file timeline** → `hkb_search(mode="recent")`
- **Need token-budgeted context for a task** → `hkb_context(topic="...")`
  - Use `hkb_search` when browsing/exploring. Use `hkb_context` when you need packed context to work with.
- **Want file suggestions for a topic** → `hkb_context(mode="suggest", topic="...")`
- **Want a chronological story** → `hkb_context(mode="narrative", topic="...")`
- **Want link graph** → `hkb_show(name="file", links=True)`

### I need to SAVE something

- **Know the target file** → `hkb_add(content="...", to="file.name")`
- **Unsure where it goes** → `hkb_add(content="...")` (auto-routes) — then handle the response (see below)
- **Preview routing first** → `hkb_search(mode="check", query="...")` to see candidates
- **Need a new file** → `hkb_add(create_file=True, to="new.name", description="...", keywords=[...])`
  then `hkb_add(content="...", to="new.name")` to add the first entry

### I need to UPDATE something

- **Amend entry content or metadata** → `hkb_update(file="f", epoch=E, new_content="...", set_status="...", add_tags="...")`
- **Archive an entry** → `hkb_update(action="archive", file="f", epoch=E)`
- **Supersede old info** → Save new entry, then `hkb_update(file="f", epoch=OLD, set_status="superseded")`
- **Bulk changes** → `hkb_update(action="batch", updates=[...])`

### I need to ORIENT myself

- **Session start** → `hkb_session(action="briefing")` — overview of files, recent activity, open tasks
- **Focused briefing** → `hkb_session(action="briefing", focus="topic")` or `view="view-name"`
- **What changed recently** → `hkb_session(action="review", after="1d")`
- **Bias subsequent searches** → `hkb_session(action="anchor", topics="auth, security")`
  - Anchoring applies a 1.5x score boost to matching files. It is NOT a filter — unanchored files still appear, just ranked lower.

### I need to MANAGE

- **Tasks** → `hkb_task(action="create/show/update/list")`
- **Health checks** → `hkb_health(checks="all", fix=True)` — finds orphan links, stale entries, sync issues
- **Reindex from disk** → `hkb_health(action="reindex")`
- **Compact old entries** → `hkb_health(action="compact", file="...", dry_run=True)`
- **Sync with S3** → `hkb_sync(action="both")`
- **Named views** → `hkb_view(action="set", name="my-view", files=["a.b", "c.d"])`

## Handling hkb_add Responses

Every `hkb_add` call returns one of three results:

| Response | Meaning | Next Step |
|----------|---------|-----------|
| `"ok"` | Entry saved. May include `broken_links` warning. | Done. Fix broken `[[links]]` if warned. |
| `"no_match"` | No file matched the content. | Create a file first: `hkb_add(create_file=True, to="name", ...)`, then retry with `to=`. |
| `"low_confidence"` | Candidates found but none confident. Lists them. | Pick one and retry with `to=`, OR create a new file. |

**Do NOT re-call `hkb_add` without setting `to=`** — auto-routing will return the same result every time.

## Writing Good Entries

### Entry metadata format

`@key: value` lines at the START of entry content are parsed as metadata. They are not stored in the content body — they become DB columns for filtering and scoring.

```
@type: finding
@status: active
@weight: high
@tags: security, auth
Connection pool exhaustion occurs under 50+ concurrent requests.
Root cause: default pool size of 10 with no timeout on checkout.
See [[infra.postgres]] for the fix applied.
```

### Types and when to use them

| @type | When to use |
|-------|-------------|
| `note` | General observations, config values, context (default) |
| `finding` | Investigation results, root causes, measurements |
| `decision` | Conclusions with rationale — exempt from staleness penalty |
| `task` | Action items — prefer `hkb_task` for lifecycle tracking |
| `milestone` | Completion markers, version releases |
| `skill` | Reusable procedures — use Problem/Solution/Context format |

### @type: skill format

Use for any procedural knowledge — workarounds, techniques, how-to steps:

```
@type: skill
@tags: docker, networking
Problem: Container can't reach host services on macOS.
Solution: Use `host.docker.internal` instead of `localhost` in container config.
Set `extra_hosts: ["host.docker.internal:host-gateway"]` in docker-compose.
Context: Only needed on macOS/Windows Docker Desktop. Linux uses --network=host.
```

### Good vs. bad entries

**Good:** Self-contained, includes why, useful months later.
```
@type: finding
BM25 search returns no results when query contains "NOT" or "AND" because
FTS5 treats them as operators. Fixed by quoting reserved words in search.py.
```

**Bad:** Vague, no context, not self-contained.
```
Fixed the search bug.
```

### Other metadata

- `@weight: high` — keeps entry prominent regardless of age. Use for ~5% of entries (architecture decisions, critical security findings). `@weight: low` for temporary workarounds.
- `@tags: comma, separated` — cross-cutting labels for filtering.
- `@author` / `@hostname` — auto-populated, never set manually.

### Content constraints

- No `>>>` or `<<<` on a line by itself (entry delimiters)
- Max entry size: 1 MiB
- One topic per entry — split multi-topic content into separate adds

## File Naming

`domain.topic[.subtopic[.focus]].md` — 2-4 dot-separated lowercase segments with hyphens. Use project name as first segment for natural `domain=` filtering. Archives: 5 segments allowed when last is "archive" (e.g. `project.auth.tokens.old.archive`).

Examples: `myproject.architecture`, `security.threat-intel.ioc-feeds`, `tasks.myproject`

## Workflow Patterns

**Session start:** `hkb_session(action="briefing")` → scan open tasks → anchor relevant topics.

**Deep dive:** `hkb_context(topic="...", max_tokens=3000)` for packed context, then `hkb_session(action="anchor", topics="...")` to bias ongoing search.

**Task tracking:** `hkb_task(action="create", title="...", file="tasks.project")` → update status as work progresses → mark completed with note.

**Superseding knowledge:** Save new entry with updated info → `hkb_update(file="f", epoch=OLD, set_status="superseded")` on the outdated entry.

**Maintenance:** `hkb_health(checks="all", fix=True)` periodically. `hkb_health(action="compact", file="...", dry_run=True)` to preview merging old clustered entries.

## Lifecycle Progressions

**Task:** `pending` → `in_progress` → `blocked` → `in_progress` → `completed`

**Investigation:** `finding(active)` → `decision` reached → original finding `superseded`

**Knowledge:** `note(active)` → better info available → `superseded` → eventually `archive`
