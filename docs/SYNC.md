# Synchronization between machines

Sync is optional. Each machine keeps a local knowledge base; an S3-compatible bucket holds the shared remote state. Git tracks local file history. No remote bucket is required for local memory and search.

## Upgrade existing installations first

The reliability update introduces **sync manifest version 2** with immutable content objects and explicit deletion records. It can read legacy manifests, but older HyperKB clients cannot safely interpret the new blob references. Upgrade and restart **every client using a bucket/prefix before an upgraded client publishes to it**. Do not mix old and new clients against the same destination.

Back up each machine's Markdown and the remote bucket before a coordinated upgrade. For a staged trial, configure a separate bucket prefix. Unknown manifest versions are rejected rather than guessed.

## Set up a destination

Install Git and the sync extras:

```bash
.venv/bin/pip install -e ".[all]"
.venv/bin/hkb sync setup
```

The setup wizard asks for your bucket, prefix, region, optional endpoint, and credentials. Use a dedicated prefix for this knowledge base. Configuration is also available through `hkb config` and the MCP `hkb_sync(action="config")` tool.

Credential overrides are `HKB_SYNC_ACCESS_KEY` and `HKB_SYNC_SECRET_KEY`. Use your environment or a hidden prompt rather than putting secrets into a saved conversation or shell command. A machine-derived encryption key is used when saving credentials with the crypto extra; it is not a substitute for operating-system access controls or portable secret management.

For a custom S3-compatible service, set `sync_endpoint_url`. Compatibility requires conditional object writes and deletes used by the lease and conditional manifest publication; an endpoint that cannot provide them should fail safely rather than pretend synchronization is exclusive.

## Preview before applying

```python
hkb_sync(action="both", dry_run=True)
hkb_sync(action="both")
hkb_sync(action="status")
```

The dry run reports pending changes. Inspect its status instead of assuming every sync attempt ran: another machine may hold the lease, or the endpoint may be unavailable.

## Choose a direction

| Action | Intended effect |
| --- | --- |
| `both` | Retrieve remote changes, preserve/merge local changes, and publish the resulting state |
| `pull` | Bring in remote changes without acknowledging unuploaded local changes |
| `push` | Publish local changes while preserving remote inventory for unrelated paths |

A stored baseline distinguishes local additions from remote deletions. A machine without a baseline does not treat every file it lacks as a local deletion. Explicit tombstones preserve deletion history.

## What happens during sync

1. Obtain a conditional remote lease with a unique owner token.
2. Read the manifest under that lease and compare it with local and previously synchronized state.
3. Download and verify remote content in staging, then merge files with their common ancestor.
4. Publish local mutations through the same storage coordination used by MCP writes.
5. Upload immutable content objects and conditionally publish the new manifest only after their contents exist; a stale inventory cannot overwrite a newer publication.
6. Record the new baseline and update the index for applied changes, including deletions.

Remote file names and hashes are validated. The live storage directory is not temporarily checked out onto a remote branch. Unrelated local files remain in the remote manifest during a push.

## Multiple clients on one machine

One running MCP process owns the background sync worker. Other clients can continue using the same local KB, and an eligible follower can take over after the leader exits. Manual sync and background sync coordinate through the storage lock and remote lease.

The normal polling interval is controlled by `sync_interval`. With watchdog installed, file changes can also request a debounced sync. Changing sync settings through MCP restarts the local worker with the updated settings. Clients started while sync was disabled need to reconnect after another process enables it to join the follower pool. For coordinated upgrades, close and restart all clients.

## Conflicts and interruptions

```python
hkb_sync(action="conflicts")
```

Review conflict records when concurrent changes need attention. The merge path preserves competing entry content rather than silently choosing one version as universally correct. A sync failure may leave successfully completed work in place; inspect status and local files before assuming the entire attempt was rolled back.

Version 2 blobs and tombstones are retained to support safe publication and recovery. Automatic garbage collection of remote historical objects and automatic squashing of local Git history are not provided by this update; plan storage retention deliberately. Do not remove blobs still referenced by a manifest or deletion record.

## Recovery and limits

The bucket is a synchronization destination, not a complete backup policy. Keep independent backups and consider versioning at your storage provider. Local Git history and retained blobs help recovery but do not protect against every operator error.

A missing manifest with existing remote objects stops synchronization. Restore the inventory from backup; legacy inventories can be explicitly rebuilt by an administrator, while immutable objects alone cannot recover their filenames or deletion history.

Sync holds the shared mutation lock while handling a snapshot, including its network operations. Indexed reads in other clients can continue, but writes may wait or time out during a slow sync. This trades write latency for a consistent first implementation.

Offline edits remain local until synchronization succeeds. A network connection from another machine to `hkb-mcp` is not part of this setup: that machine runs its own MCP process and knowledge base, then shares the configured remote state.
