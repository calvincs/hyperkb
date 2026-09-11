# HyperKB

**Pick up where your last session left off.**

HyperKB gives AI tools a persistent place for findings, decisions, tasks, and the reasons behind them. Knowledge lives in Markdown you own. Ripgrep and SQLite/BM25 make it searchable; ten MCP tools bring it into your next session.

[Website](https://hyperkb.com/) · [Documentation](docs/GETTING_STARTED.md) · [Agent guide](llm.txt) · [MCP reference](docs/MCP.md)

## Start in a few commands

With Python 3.10+ and [ripgrep](https://github.com/BurntSushi/ripgrep#installation):

```bash
git clone https://github.com/calvincs/hyperkb
cd hyperkb
python3 -m venv .venv
.venv/bin/pip install -e ".[mcp]"
.venv/bin/hkb init
```

Register the absolute path to `.venv/bin/hkb-mcp` in your MCP client:

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

The default KB is `~/.hkb/`. Use `hkb init --path ROOT` and server arguments `["--path", "ROOT"]` for another location. See [the setup guide](docs/GETTING_STARTED.md) for complete instructions.

## Save the reason, not just the setting

Through your MCP client:

```python
hkb_add(create_file=True, to="infra.postgres",
        description="Postgres connection settings and operating decisions.",
        keywords=["postgres", "connection", "pool"])
hkb_add(to="infra.postgres",
        content="@type: decision\nUse a pool limit of 20. Higher limits exhaust our worker budget under load.")
hkb_search(query="connection pool", domain="infra")
hkb_context(topic="postgres connection limits", max_tokens=2000)
```

Entries use `>>> epoch` / `<<<` delimiters and optional metadata. Topic filenames are dotted namespaces such as `infra.postgres.md`. Connect related entries with `[[app.performance]]` or `[[app.performance#epoch]]`.

## What it provides

- Human-readable Markdown storage and a rebuildable SQLite index.
- Hybrid entry search, metadata filters, complete context retrieval, session briefings, and linked narratives.
- Multiple local MCP clients with coordinated writes, separate session anchors, and source attribution.
- Entry amendments, archive companions, task lifecycles, and saved topic views.
- Optional synchronization through your S3-compatible storage, with staged merges and one background leader per local KB.
- Offline `hkb doctor` and `hkb reindex` for recovery when MCP is unavailable.

Search runs locally without an embedding model or a model API key. Entry amendments are supported; this is not an immutable append-only log. Context budgets use a response-size estimate rather than a particular model tokenizer.

## Documentation

| Guide | What you will learn |
| --- | --- |
| [Getting started](docs/GETTING_STARTED.md) | Install, connect, save, and retrieve |
| [How it works](docs/UNDERSTANDING.md) | Files, entries, links, and the shared local KB |
| [MCP & clients](docs/MCP.md) | Connection settings, ten tools, results, and retries |
| [Everyday workflows](docs/WORKFLOWS.md) | Briefings, decisions, tasks, and focused views |
| [Search & context](docs/SEARCH.md) | Modes, filters, ranking, and estimated budgets |
| [Synchronization](docs/SYNC.md) | S3 setup, directions, conflicts, and upgrades |
| [Maintenance & recovery](docs/OPERATIONS.md) | Health, reindex, backups, and compaction |
| [Configuration](docs/CONFIGURATION.md) | Settings, provenance, credentials, and extras |
| [Agent reference](SKILL.md) | Tool selection and practical call examples |

The website publishes readable versions of these canonical Markdown guides, including source links and machine-readable entry points. [Website maintenance](https://github.com/calvincs/hyperkb/blob/main/docs/WEBSITE.md) explains the build and validation workflow.

## Upgrading an existing installation

Restart every local MCP client after upgrading. **Local recording continues during outages and protocol mismatches; remote sync pauses with an upgrade warning.** Clients check the S3 protocol before syncing and preserve pending edits for reconciliation after upgrade. Existing protocol 1 stores have an explicit, previewable migration to protocol 2. Clients predating these checks need one coordinated upgrade first. See [the migration guide](docs/SYNC.md#upgrade-existing-installations-first).

## Development

```bash
.venv/bin/pip install -e ".[all,dev]"
PYTHONPATH=. .venv/bin/pytest tests/ -q
.venv/bin/pip install -r website/requirements.txt
.venv/bin/python scripts/build_site.py
.venv/bin/python scripts/check_site.py
```

The website is static HTML, CSS, and JavaScript. The only documentation renderer dependency is build-time Python-Markdown. See [CONTRIBUTING.md](https://github.com/calvincs/hyperkb/blob/main/CONTRIBUTING.md).

## License

[MIT](LICENSE).
