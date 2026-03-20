# hyperkb

A hyperconnected knowledge base with hybrid search and context-aware retrieval. Flat markdown files as source of truth, SQLite for indexing, ripgrep for speed. Designed as long-term memory for AI coding assistants via MCP (Model Context Protocol).

## Architecture

```
.md files (source of truth)
    |
    +-- ripgrep ----------> exact/regex matches (fastest)
    |
    +-- SQLite index
         +-- FTS5 --------> BM25 keyword ranking (entries + file routing)
```

**Design principles:**
- Append-only entries with epoch timestamps (never mutate, only append)
- Dotted namespace filenames replace folder hierarchy (`security.threat-intel.ioc-feeds.md`)
- YAML frontmatter describes what belongs in each file (enables auto-routing)
- Wiki-links (`[[file.name]]`, `[[file.name#epoch]]`) for cross-references
- Entry search: ripgrep + BM25. File routing: BM25 + filename matching.
- KB lives at `~/.hkb/` by default; use `--path` to override
- Sync keys encrypted at rest with machine-tied Fernet
- MCP-first: all knowledge operations go through the MCP server (10 tools); the CLI handles only admin (`hkb init`, `hkb config`, `hkb sync`)

## Prerequisites

- **Python 3.10+**
- **ripgrep** (`rg`): [Install instructions](https://github.com/BurntSushi/ripgrep#installation)
  ```bash
  # macOS
  brew install ripgrep
  # Ubuntu/Debian
  sudo apt install ripgrep
  # Cargo
  cargo install ripgrep
  ```

## Installation

### Global install (recommended for daily use)

Install `hkb` into a dedicated venv inside `~/.hkb/` so it's available everywhere:

```bash
# 1. Clone the repo
git clone https://github.com/calvincs/hyperkb ~/src/hyperkb
cd ~/src/hyperkb

# 2. Initialize global KB and create its venv
python3 -m venv ~/.hkb/venv
~/.hkb/venv/bin/pip install -e ".[all]"

# 3. Add to PATH (add this to ~/.bashrc or ~/.zshrc)
export PATH="$HOME/.hkb/venv/bin:$PATH"

# 4. Initialize the KB
hkb init

# 5. Verify
hkb --version
```

After adding the PATH line and reloading your shell (`source ~/.bashrc`), `hkb` is available from any directory.

### Development install

```bash
cd hyperkb
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[all,dev]"          # Everything + pytest
```

### Optional extras

```bash
pip install -e ".[all]"              # Everything: crypto + mcp + sync
pip install -e ".[crypto]"           # Just cryptography (key encryption)
pip install -e ".[mcp]"             # Just MCP server (Claude Code native integration)
pip install -e ".[sync]"            # Just boto3 + watchdog (multi-machine S3 sync)
pip install -e ".[dev]"              # pytest + moto (S3 mocking)
```

## Quick Start

```bash
# Initialize KB (at ~/.hkb/)
hkb init

# All knowledge operations are done through the MCP server.
# Register hkb-mcp in your Claude Code settings (see "Claude Code Integration" below).
```

## Filename Convention

```
domain.topic[.subtopic[.focus]].md
```

- 2-4 dot-separated segments
- Lowercase alphanumeric + hyphens within segments
- Dots act as hierarchy (replaces folders)
- **Suggested**: use project/workspace name as first segment for natural grouping
- Examples: `myproject.architecture.decisions`, `security.threat-intel`, `fitness.kettlebell.programming`

Using `domain="myproject"` then naturally filters to all files for that project.

## File Format

```markdown
---
name: security.threat-intel.ioc-feeds
description: >
  IOC feed sources, quality observations, and ingestion configs.
  NOT for detection rules or alert tuning.
keywords: [ioc, threat-intel, feeds, stix, taxii, misp]
links: [security.siem.correlation, security.threat-intel.enrichment]
created: 2026-02-21T10:00:00Z
compacted: ""
---

>>> 1740130800
AlienVault OTX has 6hr delay vs abuse.ch.
Switched primary IOC source for malware hashes to abuse.ch/urlhaus.
See also: [[security.siem.correlation]]
<<<

>>> 1740217200
@type: finding
@weight: high
MISP now pulling from 3 feeds. Dedup rate ~40% across feeds.
Need to investigate overlap. See [[security.threat-intel.enrichment#1740130800]]
<<<
```

## Entry Metadata

Entries can include `@key: value` metadata lines at the start of their content. These are parsed into DB columns for filtering and scoring.

### `@type` — Entry classification

| Type | Purpose | Scoring |
|------|---------|---------|
| `note` | General observations, context (default) | 1.0x |
| `finding` | Investigation results, measurements | 1.05x |
| `decision` | Conclusions with rationale | 1.1x (exempt from staleness penalty) |
| `task` | Action items (use `hkb_task`) | 1.0x |
| `milestone` | Completion markers, releases | 1.05x |
| `skill` | Reusable procedures | 1.08x |

### `@status` — Entry lifecycle

| Status | Purpose | Scoring |
|--------|---------|---------|
| `active` | Current, relevant (default) | 1.0x |
| `pending` | Awaiting action | 1.08x boost |
| `in_progress` | Work underway | 1.05x boost |
| `blocked` | Waiting on dependency | 0.95x |
| `completed` | Done | 0.88x dampen |
| `superseded` | Replaced by newer info | 0.85x dampen |
| `resolved` | Issue fixed | 0.9x dampen |
| `cancelled` | Abandoned | 0.65x dampen |
| `archived` | Moved to .archive file | 0.7x dampen |

### `@weight` — Entry importance

| Weight | Purpose | Scoring |
|--------|---------|---------|
| `high` | Critical info that should stay prominent | 1.15x (exempt from staleness penalty) |
| `normal` | Standard importance (default) | 1.0x |
| `low` | Temporary workarounds, version-specific notes | 0.8x |

### `@tags` — Comma-separated labels

```
@tags: security, urgent, migration
```

Tags are searchable and filterable. Use them for cross-cutting concerns that don't fit in filenames.

### `@author` / `@hostname` — Entry provenance (auto-populated)

Every new entry automatically records:
- `@author`: The AI client or tool that created the entry. Determined by: `HKB_SOURCE` env var > `config.default_source` > `"unknown"`.
- `@hostname`: The machine hostname at write time (via `socket.gethostname()`).

These fields are never set manually — they're injected by `_append_entry()`. Use `author` and `hostname` filter params on `hkb_search` to scope results by origin.

### Workflow progressions

**Feature/task lifecycle:**
```
@type: task, @status: pending
  → @status: in_progress  (work begins)
  → @status: blocked      (dependency found)
  → @status: in_progress  (unblocked)
  → @status: completed    (done)
```

**Investigation lifecycle:**
```
@type: finding, @status: active
  → @type: decision (conclusion reached)
  → original finding: @status: superseded
```

## MCP Tools Reference (10 tools)

Each tool covers a domain with sub-actions via `action=` or `mode=` parameters.

| Tool | Purpose | Sub-actions/modes |
|------|---------|-------------------|
| `hkb_search` | Find entries, timeline, routing preview | `mode`: hybrid, rg, bm25, recent, check |
| `hkb_show` | Read files, list files, link graph | `name=""` → list; `links=True` → graph |
| `hkb_add` | Add entry or create file | `create_file=True` → new file |
| `hkb_update` | Amend, archive, or batch update | `action`: update, archive, batch |
| `hkb_task` | Task lifecycle | `action`: create, show, update, list |
| `hkb_sync` | Sync operations | `action`: push, pull, both, status, config, conflicts |
| `hkb_session` | Briefing, review, anchor | `action`: briefing, review, anchor |
| `hkb_context` | Token-budgeted retrieval | `mode`: packed, suggest, narrative |
| `hkb_view` | Named file groupings | `action`: set, list |
| `hkb_health` | Maintenance operations | `action`: check, reindex, compact |

See `SKILL.md` for detailed parameter reference.

## Context-Aware Retrieval

These features transform hyperkb from "search and dump" into intelligent context delivery — the AI gets exactly what it needs without burning tokens on irrelevant content.

### Token-Budgeted Context (`hkb_context`)

Searches for a topic and packs the most relevant entries into a token budget using a greedy knapsack.

```
hkb_context(topic="auth security", max_tokens=3000)
hkb_context(topic="refactor auth", mode="suggest")     # File suggestions
hkb_context(topic="sync impl", mode="narrative", depth="1")  # Story reconstruction
```

### Session Management (`hkb_session`)

```
hkb_session(action="briefing")                         # Session startup overview
hkb_session(action="briefing", focus="auth")           # Topic-focused briefing
hkb_session(action="review", after="1d")               # Session audit
hkb_session(action="anchor", topics="auth, security")  # Set topic bias (1.5x boost)
```

### Named Context Views (`hkb_view`)

```
hkb_view(action="set", name="auth-work", files=["auth.tokens", "auth.sessions"])
hkb_session(action="briefing", view="auth-work")       # Briefing scoped to view
hkb_view(action="list")                                 # List all views
```

## Search & Scoring

### Search Modes

| Mode | Engine | Best For | Speed |
|------|--------|----------|-------|
| `hybrid` | rg + FTS5 | General queries | Medium |
| `rg` | ripgrep | Exact strings, regex, known values | Fastest |
| `bm25` | SQLite FTS5 | Keyword queries, term ranking | Fast |

### Scoring Pipeline

Search results pass through a multi-stage scoring pipeline:

1. **Source weighting**: rg (0.5x), BM25 (0.5x) — configurable via `rg_weight`/`bm25_weight`
2. **Deduplication**: Same entry from multiple sources → merged score
3. **Metadata enrichment**: rg results get status/type/tags from DB
4. **Recency boost**: 80% relevance + 20% recency-weighted (exponential decay, configurable half-life of 180 days)
5. **Staleness penalty**: Active entries older than 2x half-life get dampened (floor 0.7). Decisions and `@weight: high` entries are exempt.
6. **Type boost**: Decisions 1.1x, skills 1.08x, findings 1.05x, milestones 1.05x
7. **Status boost/dampen**: Pending 1.08x, in_progress 1.05x, completed 0.88x, cancelled 0.65x, etc.
8. **Weight boost**: High 1.15x, normal 1.0x, low 0.8x
9. **Anchor boost** (if active): 1.5x for files matching session anchors

The `recency_half_life_days` config key (default 180) controls how quickly old entries fade. Set lower for fast-moving projects, higher for reference-heavy KBs.

## Claude Code Integration

### MCP Server

The MCP server lets Claude Code call hyperkb tools natively — no shell, no per-command
permission prompts, structured JSON in/out, and a persistent DB connection for the session.

The server auto-initializes the KB on first launch if `hkb init` hasn't been run.

**1. Install with MCP support:**

```bash
~/.hkb/venv/bin/pip install -e ".[all]"   # includes mcp[cli] dependency
```

**2. Register the server** in your project's `.mcp.json` (or global `~/.claude/.mcp.json`):

```json
{
  "mcpServers": {
    "hyperkb": {
      "command": "/home/YOU/.hkb/venv/bin/hkb-mcp",
      "args": []
    }
  }
}
```

Replace `/home/YOU` with your actual home directory path. To use a non-default KB location,
add `"args": ["--path", "/some/dir"]`.

**3. Auto-approve the MCP tools** in `.claude/settings.local.json`:

```json
{
  "permissions": {
    "allow": [
      "mcp__hyperkb__hkb_search",
      "mcp__hyperkb__hkb_show",
      "mcp__hyperkb__hkb_add",
      "mcp__hyperkb__hkb_update",
      "mcp__hyperkb__hkb_task",
      "mcp__hyperkb__hkb_sync",
      "mcp__hyperkb__hkb_session",
      "mcp__hyperkb__hkb_context",
      "mcp__hyperkb__hkb_view",
      "mcp__hyperkb__hkb_health"
    ]
  }
}
```

**4. Restart Claude Code.** You'll be prompted to approve the MCP server on first launch.
After that, all 10 tools are available as native tools with no per-call prompts.

### Skill Files for AI Agents

hyperkb ships skill files that teach AI agents how to use the 10 MCP tools effectively.
These are decision guides, not API references — they teach WHEN to use each tool and
HOW to handle responses.

| File | Purpose |
|------|---------|
| `SKILL.md` | Main reference skill — tool selection, entry writing, error handling |
| `.claude/skills/rem/SKILL.md` | `/rem` slash command — natural language save/recall |

**Claude Code:**

```bash
# Skills auto-load from the project's SKILL.md and .claude/skills/ directory.
# For global availability (outside the hyperkb repo), copy to ~/.claude/skills/:
mkdir -p ~/.claude/skills/hyperkb ~/.claude/skills/rem
cp SKILL.md ~/.claude/skills/hyperkb/SKILL.md
cp .claude/skills/rem/SKILL.md ~/.claude/skills/rem/SKILL.md
```

**OpenCode:**

```bash
# Copy skills + instructions to OpenCode's global config:
cp -r integrations/opencode/skills/hyperkb ~/.config/opencode/skills/hyperkb
cp -r integrations/opencode/skills/rem     ~/.config/opencode/skills/rem
cp integrations/opencode/instructions/hyperkb.md ~/.config/opencode/instructions/

# Register instructions in ~/.config/opencode/opencode.json:
# "instructions": ["~/.config/opencode/instructions/hyperkb.md"]
```

**Other MCP Clients / Agent Swarms:**

Any MCP-capable client can use hyperkb. The key files:
- **MCP registration**: Point your client at `hkb-mcp` binary (see MCP Server section above)
- **Tool guidance**: Feed `SKILL.md` content as system instructions or context
- **The server's built-in `instructions` field** (SERVER_INSTRUCTIONS) provides basic
  tool guidance automatically — skills add deeper decision logic on top

For agent swarms: each agent that connects to the same `hkb-mcp` instance shares
the same KB. Session-level state (anchors) is per-connection. The KB itself is
concurrency-safe (thread-locked SQLite + file-level write locks).

## Admin CLI

```bash
hkb init [--path DIR]                              # Initialize KB
hkb config KEY [VALUE] [--set]                     # View/set config
hkb sync setup [--path DIR]                        # Interactive S3 sync wizard
hkb sync status [--path DIR]                       # Quick sync state check
```

## Configuration

```bash
hkb config rg_weight 0.5               # Search weight tuning
hkb config bm25_weight 0.5
hkb config recency_half_life_days 180   # Scoring: how fast old entries fade
hkb config sync_access_key --set        # Sensitive field (hidden prompt)
```

### Config Keys

| Key | Default | Purpose |
|-----|---------|---------|
| `rg_weight` | `0.5` | Ripgrep result weight in hybrid search |
| `bm25_weight` | `0.5` | BM25/FTS5 result weight |
| `route_confidence_threshold` | `0.6` | Auto-routing: min score for confident match |
| `recency_half_life_days` | `180` | Search scoring: half-life for recency decay |
| `max_entry_size` | `1048576` | Max entry size in bytes (1 MiB) |
| `rg_timeout` | `10.0` | Ripgrep timeout (seconds) |
| `sync_enabled` | `false` | Enable multi-machine S3 sync |
| `sync_bucket` | — | S3 bucket name |
| `sync_prefix` | `hkb/` | S3 key prefix |
| `sync_region` | — | AWS region |
| `sync_endpoint_url` | — | Custom S3 endpoint (MinIO, Backblaze, etc.) |
| `sync_access_key` | — | S3 access key (encrypted at rest) |
| `sync_secret_key` | — | S3 secret key (encrypted at rest) |
| `sync_interval` | `60` | Background sync interval (seconds) |
| `sync_squash_threshold` | `20` | Git history squash threshold |
| `default_source` | — | Default author for entries (overridden by `HKB_SOURCE` env var) |

Sensitive fields (`sync_access_key`, `sync_secret_key`) are masked on display and encrypted at rest. Use `--set` for hidden prompt input. Environment variable overrides: `HKB_SYNC_ACCESS_KEY`, `HKB_SYNC_SECRET_KEY`, `HKB_SOURCE`.
```

## Multi-Machine Sync

Sync your KB across multiple machines using an S3-compatible bucket as the remote.
Git is used locally for change tracking and three-way merge; S3 provides simple remote storage.

### Setup

```bash
# Install sync dependencies
pip install -e ".[sync]"

# Interactive setup wizard
hkb sync setup

# Or configure manually via MCP:
hkb_sync(action="config", key="sync_bucket", value="my-hkb-bucket")
hkb_sync(action="config", key="sync_region", value="us-east-1")
hkb_sync(action="config", key="sync_access_key", value="AKIA...")
hkb_sync(action="config", key="sync_secret_key", value="...")
hkb_sync(action="config", key="sync_enabled", value="true")
```

### How It Works

- `~/.hkb/storage/` becomes a local git repo (auto-initialized on sync setup)
- Every store write auto-commits the changed file(s)
- On sync: download remote changes from S3, apply on a temp branch, `git merge` into main
- Git auto-merges concurrent appends (the common case). Entry-aware conflict resolver handles the rare cases git can't.
- Push merged state to S3. Squash old git history to prevent `.git` bloat.

### Usage

```
# Manual sync
hkb_sync(action="both")             # Push + pull (default)
hkb_sync(action="push")             # Upload local changes only
hkb_sync(action="both", dry_run=True)  # Preview without applying

# Check status
hkb_sync(action="status")

# View/clear conflict history
hkb_sync(action="conflicts")
hkb_sync(action="conflicts", conflict_action="clear")
```

The MCP server automatically starts a background sync worker when sync is enabled,
polling the remote every `sync_interval` seconds (default: 60). With `watchdog`
installed, local filesystem changes also trigger sync with a 5-second debounce.

### S3-Compatible Storage

Works with AWS S3, MinIO, Backblaze B2, and any S3-compatible storage:

```bash
# MinIO example
hkb config sync_endpoint_url http://minio.local:9000
hkb config sync_bucket my-kb-bucket
```

Environment variable overrides: `HKB_SYNC_ACCESS_KEY`, `HKB_SYNC_SECRET_KEY`.

## Workflow Examples

```python
# Session start
hkb_session(action="briefing")                                # Overview
hkb_session(action="briefing", focus="auth")                  # Focused on a topic
hkb_session(action="briefing", view="my-project", focus="migration")  # Scoped + focused

# Store a finding (know the file)
hkb_add(content="@type: finding\n@weight: high\nConnection pool exhaustion under 50+ concurrent requests.", to="infra.postgres")

# Store a finding (unsure where)
hkb_search(mode="check", query="...") → hkb_add(content="...", to="<suggested>")

# Search
hkb_search(query="connection pool", domain="infra")

# Get focused context for a topic (token-budgeted)
hkb_context(topic="auth token security", max_tokens=3000)

# Get file suggestions before starting work
hkb_context(mode="suggest", topic="refactor auth middleware")

# Set session anchors to bias all subsequent calls
hkb_session(action="anchor", topics="auth, security")

# Build a topic narrative across files
hkb_context(mode="narrative", topic="database migration", depth="1")

# Cross-reference
hkb_add(content="See [[other.file#1740130800]]", to="my.file")

# Entry lifecycle
hkb_update(file="f", epoch=E, set_status="superseded")
hkb_update(action="archive", file="f", epoch=E)

# Tasks
hkb_task(action="create", title="Fix connection pool", file="tasks.myproject")
hkb_task(action="update", file="tasks.myproject", epoch=E, status="completed", note="Done in PR #42")

# Named views
hkb_view(action="set", name="auth-work", files=["auth.tokens", "auth.sessions", "infra.postgres"])
hkb_session(action="briefing", view="auth-work")

# Health
hkb_health(checks="all", fix=True)

# Sync
hkb_sync(direction="both")
```

## License

MIT License. See [LICENSE](LICENSE).
