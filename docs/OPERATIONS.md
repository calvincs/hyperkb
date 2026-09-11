# Maintenance & recovery

Your Markdown files are the primary knowledge store. SQLite is a rebuildable index. Keep independent backups of important knowledge even when sync is enabled.

## Check health without MCP

```bash
.venv/bin/hkb doctor
.venv/bin/hkb doctor --path /absolute/path/to/knowledge-root
```

`doctor` inspects SQLite and reports knowledge-base health. It does not repair or replace the index, run schema migrations, or checkpoint it. SQLite may create normal WAL reader bookkeeping files. Configuration and initialization problems are reported with recovery advice.

Through MCP:

```python
hkb_health(checks="all")
hkb_health(checks="all", fix=True)
```

Read the returned checks and fixes. A clean count-based check does not prove that every external file edit is reflected in the index.

## Rebuild the index

```bash
.venv/bin/hkb reindex
.venv/bin/hkb reindex --path /absolute/path/to/knowledge-root
```

Reindex reads Markdown and rebuilds entries, full-text search, metadata, and links from a coordinated snapshot. It does not reset your configuration. A corrupt index is preserved under a quarantine name for diagnosis before a replacement is built. The MCP server can also recover a missing index during startup.

Use the offline command when MCP cannot start. If the Markdown itself is incomplete or corrupt, index rebuilding cannot invent the missing text: restore it from a backup or known history first.

## Recover from a protocol mismatch

Continue recording locally. `hkb sync status` and `hkb_sync(action="status")` report the remote protocol and upgrade instructions. A mismatch stops remote transfers and preserves pending edits; disconnecting does not erase that warning. Upgrade/restart the client, preview and apply a supported store migration if required, then preview sync before reconciling. Keep `.hkb/sync` with your Markdown so the previous common baseline survives. See [the migration and reconciliation workflow](SYNC.md#reconcile-changes-recorded-while-sync-was-paused).

## Back up safely

For a simple consistent backup, close the clients using this KB and wait for their processes to exit before copying the `.hkb` directory. Retain `storage`, configuration, and sync state. Treat saved configuration as potentially sensitive. Store the backup outside the live storage directory.

For long-term content portability, the Markdown files are sufficient to reconstruct the searchable knowledge. Git history and synchronization baselines contain additional recovery state. Encrypted credentials are tied to the original machine; provision credentials separately on another machine.

## Move or restore a knowledge base

Restore the `.hkb` directory underneath the intended root, update every client to use that root, and run `hkb doctor` followed by `hkb reindex` if needed. The configured root is resolved from the location you load, so an old absolute root in a copied configuration does not redirect storage to the previous directory.

A new machine should have its own credentials and explicit sync setup. Read the [sync migration rules](SYNC.md#upgrade-existing-installations-first) before connecting a restored copy to a shared bucket.

## Update or archive an entry

```python
hkb_update(file="infra.postgres", epoch=1789142400, set_status="superseded")
hkb_update(action="archive", file="infra.postgres", epoch=1789142400)
```

Replace the illustrative epoch with the actual entry epoch. Archive publication preserves the destination copy before removing the source entry, and retries check for an existing equivalent archived entry. Provenance and link information are retained. Conflicting archive content is surfaced rather than overwritten.

File replacement uses a temporary file and atomic rename. Multi-file operations preserve content on interruption, but this is not a general transaction spanning every filesystem, Git, and network operation.

## Group old prose carefully

```python
hkb_health(action="compact", file="app.observations", dry_run=True)
```

Preview first. Compaction groups eligible old prose by time, concatenates it, and archives the originals. It is not an AI summary and does not promise fewer tokens. Tasks, decisions, and already-grouped entries are excluded to protect their meaning. Only run the non-dry-run operation when that grouping fits your workflow.

## Operational boundaries

- Use HyperKB APIs for coordinated mutations. An external editor does not acquire the storage lock; avoid editing the same file while clients are writing, then reindex.
- Shared local storage assumes filesystem locking and atomic replacement work correctly. Multi-machine use should go through sync rather than sharing an SQLite index over a network filesystem.
- A client cancellation is not proof that a write was undone. Check the entry before retrying.
- Entry epochs are identifiers within a file, not a full revision history. Optimistic revision checks and durable idempotency receipts are not part of this release.
- Synchronization and shutdown can wait for bounded file, Git, or network operations. Do not routinely terminate servers with `kill -9`.
