# Your first knowledge base

Install HyperKB, connect one MCP client, and save a decision you want to remember in the next session.

## Requirements

- Python 3.10 or newer.
- [ripgrep](https://github.com/BurntSushi/ripgrep#installation) (`rg`) on your PATH for literal search. BM25 remains available when ripgrep is absent.
- An MCP client that can launch a local stdio server.
- Git and the optional sync dependencies if you want synchronization between machines.

The shell commands below use Linux/macOS paths. Local search does not require a model download, an embedding service, or a model API key. Install ripgrep using your system package manager, for example `brew install ripgrep` on macOS or `sudo apt install ripgrep` on Debian/Ubuntu.

## Install from source

```bash
git clone https://github.com/calvincs/hyperkb
cd hyperkb
python3 -m venv .venv
.venv/bin/pip install -e ".[mcp]"
.venv/bin/hkb init
```

This creates your knowledge base at `~/.hkb/`. The virtual environment belongs to this source checkout; keep the checkout at a stable location. To install the synchronization and encrypted-credential extras as well, use `.[all]` instead of `.[mcp]`.

## Connect your client

Find the absolute executable path:

```bash
pwd
```

Append `/.venv/bin/hkb-mcp` to that directory. Put the resulting absolute path in your MCP client's server configuration. For clients using the `mcpServers` format:

```json
{
  "mcpServers": {
    "hyperkb": {
      "command": "/absolute/path/to/hyperkb/.venv/bin/hkb-mcp",
      "env": {"HKB_SOURCE": "my-client"}
    }
  }
}
```

Replace the path and client label. Do not use `~` inside the executable path: a client may launch the command without shell expansion. The client starts HyperKB as a subprocess. A fresh server can initialize the default knowledge base automatically if you skipped `hkb init`.

For the full connection details and multiple clients, read [MCP & clients](MCP.md).

## Save one useful decision

These examples are MCP tool calls, not shell commands. Ask your agent to create a topic file:

```python
hkb_add(
    create_file=True,
    to="infra.postgres",
    description="Postgres connection settings and the reasons behind them.",
    keywords=["postgres", "connection", "pool"]
)
```

Then save a decision with its rationale:

```python
hkb_add(
    to="infra.postgres",
    content="@type: decision\nUse a connection pool limit of 20. Higher limits exhaust our worker budget under load."
)
```

The response includes the file and an entry epoch. Keep that pair when you need to update or link to this entry.

## Retrieve it in another session

```python
hkb_search(query="connection pool", domain="infra")
hkb_context(topic="postgres connection limits", max_tokens=2000)
hkb_show(name="infra.postgres")
```

The Markdown file lives at `~/.hkb/storage/infra.postgres.md`. You can read it in an ordinary text editor. Search uses a rebuildable SQLite index alongside ripgrep.

## Choose another location

```bash
.venv/bin/hkb init --path /absolute/path/to/my-knowledge
```

Use the same root in your MCP configuration:

```json
{
  "command": "/absolute/path/to/hyperkb/.venv/bin/hkb-mcp",
  "args": ["--path", "/absolute/path/to/my-knowledge"],
  "env": {"HKB_SOURCE": "my-client"}
}
```

The root is the directory **containing** `.hkb`, not `.hkb/storage` itself.

## If something goes wrong

Run `hkb doctor` from your virtual environment for a read-only health report. Use `hkb reindex` to rebuild the index from Markdown, even if MCP cannot start. Both accept `--path ROOT`. See [maintenance & recovery](OPERATIONS.md) before moving or restoring data.
