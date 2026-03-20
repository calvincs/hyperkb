# hyperkb + OpenCode Integration

This directory contains everything needed to use hyperkb with [OpenCode](https://opencode.ai).

## Prerequisites

- hyperkb installed: `pip install hyperkb` (or from source)
- A knowledge base initialized: `hkb init`
- OpenCode installed and working

## Setup

### 1. Register the MCP Server

Add the hyperkb MCP server to your global OpenCode config at `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "hyperkb": {
      "type": "local",
      "command": ["/path/to/your/.hkb/venv/bin/hkb-mcp"],
      "enabled": true
    }
  }
}
```

> **WARNING:** Do NOT pass `--path ~/.hkb` — the `--path` argument expects
> the **parent** directory containing `.hkb/`, not the `.hkb/` directory itself.
> Omitting `--path` defaults to the global KB at `~/.hkb/`. If you need to
> specify a path explicitly, use `--path /home/youruser` (your home directory).

### 2. Install Skills

Copy the skill directories to OpenCode's global skills location:

```bash
cp -r skills/hyperkb ~/.config/opencode/skills/hyperkb
cp -r skills/rem     ~/.config/opencode/skills/rem
```

Skills are loaded on-demand when the model calls the `skill` tool. OpenCode
shows available skills in an `<available_skills>` section so the model knows
they exist.

### 3. Install Persistent Instructions

Copy the instruction file and register it in your config:

```bash
cp -r instructions/hyperkb.md ~/.config/opencode/instructions/hyperkb.md
```

Then add the `instructions` array to `~/.config/opencode/opencode.json`:

```json
{
  "instructions": ["~/.config/opencode/instructions/hyperkb.md"]
}
```

This injects HKB guidance into **every** session automatically, telling the
model to call `hkb_session(action="briefing")` at startup and providing a quick
tool reference. This is critical for models that won't proactively load skills
on their own.

### 4. Auto-Approve Permissions

Add permissions to avoid approval prompts on every MCP tool call:

```json
{
  "permission": {
    "skill": "allow",
    "mcp__hyperkb__hkb_search": "allow",
    "mcp__hyperkb__hkb_show": "allow",
    "mcp__hyperkb__hkb_add": "allow",
    "mcp__hyperkb__hkb_update": "allow",
    "mcp__hyperkb__hkb_task": "allow",
    "mcp__hyperkb__hkb_sync": "allow",
    "mcp__hyperkb__hkb_session": "allow",
    "mcp__hyperkb__hkb_context": "allow",
    "mcp__hyperkb__hkb_view": "allow",
    "mcp__hyperkb__hkb_health": "allow"
  }
}
```

### 5. Complete Example Config

See [opencode.example.json](opencode.example.json) for a full working config.

## What Each File Does

```
integrations/opencode/
  instructions/
    hyperkb.md          # Always-loaded session instructions (injected every session)
  skills/
    hyperkb/SKILL.md    # Full tool reference, loaded on-demand via skill() call
    rem/SKILL.md        # Quick save/recall skill, loaded on-demand
  opencode.example.json # Complete example configuration
  README.md             # This file
```

| Component | When Loaded | Purpose |
|-----------|-------------|---------|
| `instructions/hyperkb.md` | Every session, automatically | Tells model HKB exists, call briefing at start, quick tool table |
| `skills/hyperkb/SKILL.md` | On-demand when model calls `skill("hyperkb")` | Decision guide for all 10 MCP tools, entry writing, error handling |
| `skills/rem/SKILL.md` | On-demand when user types `/rem` | Quick save/recall with intent classification |

## Troubleshooting

### MCP tools return empty results (0 files, 0 entries)

The most common cause is the `--path` bug. If you passed `--path ~/.hkb`, the
server creates a nested empty KB at `~/.hkb/.hkb/` instead of reading the real
data at `~/.hkb/storage/`. Fix: remove the `--path` argument entirely.

### Model doesn't use HKB tools

Check that:
1. The `instructions` array points to a valid file
2. The instruction file tells the model to call briefing at session start
3. Skills are installed at `~/.config/opencode/skills/`

### Model asks for permission on every tool call

Add the `permission` block from step 4 above to your `opencode.json`.

## Differences from Claude Code Setup

| Aspect | Claude Code | OpenCode |
|--------|------------|----------|
| MCP config | `~/.claude/.mcp.json` | `~/.config/opencode/opencode.json` under `mcp` key |
| Skills location | `~/.claude/skills/<name>/SKILL.md` | `~/.config/opencode/skills/<name>/SKILL.md` |
| Persistent instructions | CLAUDE.md + system prompt | `instructions` array in `opencode.json` |
| Permissions | `~/.claude/settings.local.json` | `permission` block in `opencode.json` |
| Skill name rules | Free-form | Must match `^[a-z0-9]+(-[a-z0-9]+)*$` |
