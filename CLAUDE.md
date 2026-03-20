# CLAUDE.md

**hyperkb** is a flat-file knowledge base with hybrid search (ripgrep + BM25 + optional vector). Markdown files with YAML frontmatter in `~/.hkb/storage/` are the source of truth; SQLite is a rebuildable index. The `hkb` CLI handles only admin (`init`, `config`); all knowledge operations run through the MCP server (`hkb-mcp`). Architecture details, design decisions, search algorithms, and tech debt are documented in the `hkb_*` MCP tools — use `hkb_search` and `hkb_show` on the `hyperkb.*` files to find specifics.

Data flow: `mcp_server.py` → `store.py` → `db.py` + `search.py` + `format.py`. Entry search uses ripgrep + FTS5 BM25 only (no vectors on entries). File routing uses BM25 + vector on file metadata + optional LLM fallback. All optional deps (sentence-transformers, anthropic, cryptography, mcp) degrade gracefully. Python 3.10+, dataclasses (no Pydantic), sqlite3 stdlib (no ORM), relative imports throughout.

## Development

```bash
pip install -e ".[all,dev]"                    # Setup (requires ripgrep on PATH)
.venv/bin/pytest tests/ -v                     # MUST use .venv/bin/pytest
hkb init && hkb config embedding_model         # Admin CLI
```

No linter or formatter configured. Tests use `tmp_path` + `monkeypatch.setenv("HOME", ...)` for isolation; shared fixtures in `tests/conftest.py`.
