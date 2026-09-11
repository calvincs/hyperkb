# MCP & clients

HyperKB exposes ten tools through a local stdio MCP server. The client launches `hkb-mcp`; no public HTTP endpoint or listening port is required.

## Connect a client

Install the MCP extra, then configure an absolute executable path:

```json
{
  "mcpServers": {
    "hyperkb": {
      "command": "/absolute/path/to/hyperkb/.venv/bin/hkb-mcp",
      "args": [],
      "env": {"HKB_SOURCE": "editor-a"}
    }
  }
}
```

The exact settings file and wrapper keys depend on your client. Reuse the executable, arguments, and environment values in its stdio server configuration. The current runtime supports the MCP Python SDK 1.x API; both package extras constrain it to `>=1.30,<2`.

## Use multiple clients

Register the same executable and KB root in each client. Give each a different `HKB_SOURCE`, such as `editor-a` and `terminal-b`. No shared server port or handoff script is needed.

Clients share entries and index state. Session anchors remain in the individual process and reset when it exits. `HKB_SOURCE` identifies new entries; it is not an authentication boundary. Do not give an untrusted process access to your knowledge directory.

To share a nondefault KB root, include:

```json
"args": ["--path", "/absolute/path/to/knowledge-root"]
```

Restart every client when upgrading the executable. Avoid running an old server alongside an upgraded server; old code does not participate in the new coordination protocol.

For sync-enabled clients, each tool call checks the S3 protocol. Local knowledge operations continue during outages or mismatches and include an `_protocol` warning. Search results, normally a list, use `{ "result": [...], "_protocol": {...} }` while a warning is present. Packed context counts diagnostic metadata in its token estimate; if the mandatory notice alone exceeds the budget, it returns no entries and explicitly reports `budget_exceeded`. Remote sync calls fail with an actionable protocol error until compatibility is verified. Every client performs its own check, including background-worker followers. See [sync and migration](SYNC.md#upgrade-existing-installations-first).

## Tool map

| Tool | Use it for | Main modes or actions |
| --- | --- | --- |
| `hkb_search` | Search entries, view a timeline, preview routing | `hybrid`, `rg`, `bm25`, `recent`, `check` |
| `hkb_show` | Read a file, list topic files, inspect links | Empty `name` lists files; `links=True` adds a graph |
| `hkb_add` | Append an entry or create a topic | `create_file=True` creates a file |
| `hkb_update` | Amend content or metadata, archive, batch changes | `update`, `archive`, `batch` |
| `hkb_task` | Create and track tasks | `create`, `show`, `update`, `list` |
| `hkb_sync` | Synchronize or inspect sync configuration | `both`, `push`, `pull`, `status`, `config`, `conflicts` |
| `hkb_session` | Start, focus, and review a session | `briefing`, `anchor`, `review` |
| `hkb_context` | Retrieve focused context | `packed`, `suggest`, `narrative` |
| `hkb_view` | Save named groups of files | `set`, `list` |
| `hkb_health` | Check, rebuild, or group old entries | `check`, `reindex`, `compact` |

For exact parameter names, discover the running server's `tools/list`. The [agent reference](../SKILL.md) and [workflow examples](WORKFLOWS.md) explain common combinations.

## Entry metadata

Put metadata at the start of entry content, before prose:

```text
@type: finding
@status: active
@weight: high
@tags: postgres, latency
The connection pool exhausted the worker budget under load.
```

| Field | Common values |
| --- | --- |
| `@type` | `note`, `finding`, `decision`, `task`, `milestone`, `skill` |
| `@status` | `active`, `pending`, `in_progress`, `blocked`, `completed`, `superseded`, `resolved`, `cancelled`, `archived` |
| `@weight` | `high`, `normal`, `low` |
| `@tags` | Comma-separated labels |
| `@author` | Normally populated from the client's `HKB_SOURCE` or default source |
| `@hostname` | Normally populated from the writing machine |

Entry identity is the file name plus its epoch. Entries arriving within the same second may receive different epochs to avoid collisions. Read the actual epoch from the response rather than predicting it.

## Tool results and retries

Tool helpers return JSON text. Check the returned status and required fields. Routing can return `no_match` or `low_confidence`: choose a topic or create an appropriate file instead of assuming the entry was saved. Invalid action and mode values are rejected. Batch updates process at most 50 items and report `requested`, `processed`, `skipped`, and `truncated`. Task listing accepts `top` (default 100, maximum 500) and `offset`; follow `next_offset` while `has_more` is true.

A lost response does not prove a write failed. Before retrying an uncertain append, inspect recent entries or search for the content. HyperKB does not yet provide durable idempotency keys for mutations.

Blocking database, file, Git, and network work runs through a bounded worker dispatcher so it does not stall the MCP event loop. Cancellation may leave a write finishing safely in its worker; it is not a rollback guarantee. Shutdown drains active operations before closing the store.

## Troubleshooting connection failures

Check the executable path, install the MCP extra, and try `hkb doctor` from the same environment. If the index is missing or corrupt, the server can rebuild it from Markdown during startup; `hkb reindex` is also available offline. Configuration errors should be corrected through `hkb config` or the existing configuration file. See [maintenance & recovery](OPERATIONS.md).
