# HyperKB development

HyperKB is a local Markdown knowledge base with ripgrep + SQLite/FTS5 search. Python 3.10+. Knowledge operations run through ten stdio MCP tools; the CLI provides administration and offline recovery.

Read README.md and docs/ before changing behavior. Canonical user documentation is kept in docs/*.md. The website is built from an explicit public catalog; do not publish CODE_REVIEW.md, private KB files, or arbitrary repository contents.

## Architecture

`mcp_server.py` dispatches bounded blocking work to `store.py`. The store coordinates atomic Markdown writes, `db.py`, `format.py`, and `search.py`. `locking.py` coordinates storage mutations across processes. `sync.py` and `remote.py` handle optional S3 synchronization with one elected local background worker. Session anchors and process provenance remain separate per MCP client.

Markdown is the source of truth; SQLite is rebuildable. Entries can be amended. Search does not use embedding vectors. New sync manifests use version 2 immutable blob references; all clients sharing a destination must be upgraded together. Keep migration documentation accurate.

## Validation

Use `.venv/bin/pytest tests/ -q` with the checkout on PYTHONPATH. Tests use temporary knowledge roots and mocked S3, never a user's real KB. Live stdio/multiprocess tests require an environment that permits event-loop wakeups and subprocesses.

For the website, install `website/requirements.txt`, run `scripts/build_site.py`, then `scripts/check_site.py` and the renderer tests in `website/tests`. Review the home page, documentation hub, and representative articles at desktop and narrow widths. Preserve reduced-motion support, pause controls, no-JavaScript readability, source links, and public Markdown URLs.
