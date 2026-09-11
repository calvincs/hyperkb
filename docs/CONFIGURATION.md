# Configuration

HyperKB stores settings in `.hkb/config.json` beneath the knowledge root. The default root is your home directory.

## Read and change a setting

```bash
hkb config rg_weight
hkb config rg_weight 0.5
hkb config default_source my-client
```

Add `--path /absolute/path/to/knowledge-root` for a nondefault KB. Configuration writes are validated and published atomically. Use `--path` to select a knowledge base rather than changing the stored `root` field.

## Common settings

| Setting | Purpose |
| --- | --- |
| `rg_weight` | Contribution from ripgrep matches |
| `bm25_weight` | Contribution from BM25 matches |
| `route_confidence_threshold` | Minimum confidence for automatic routing |
| `rg_timeout` | Ripgrep timeout in seconds |
| `max_entry_size` | Maximum accepted entry bytes |
| `recency_half_life_days` | Recency scoring timescale |
| `default_source` | Default author label when `HKB_SOURCE` is absent |
| `sync_enabled` | Enable optional remote synchronization |
| `sync_bucket`, `sync_prefix` | Remote destination |
| `sync_region`, `sync_endpoint_url` | Region and optional S3-compatible endpoint |
| `sync_interval` | Background synchronization interval |
| `sync_squash_threshold` | Retained legacy setting; automatic Git history squashing is disabled in the new sync engine |

The running configuration is the authority for defaults. Changes made through the admin CLI generally require client restart for the already-running process to use them. Sync settings are refreshed by the sync engine; MCP sync configuration also restarts its local worker.

## Client-specific provenance

Set `HKB_SOURCE` in each client's process environment. It takes precedence over `default_source`. HyperKB records the machine hostname automatically.

The label identifies an origin for filtering. It is not a verified identity or an access-control rule. Do not share environment credentials merely to give clients the same label.

## Credentials

```bash
hkb config sync_access_key --set
hkb config sync_secret_key --set
```

The hidden prompt avoids storing the secret in a shell command. You can also provide `HKB_SYNC_ACCESS_KEY` and `HKB_SYNC_SECRET_KEY` through your environment. Values shown by configuration tools are masked.

The crypto extra encrypts saved credentials using machine-derived key material. Without the optional dependency, saving credentials can fall back to plaintext with a warning. Protect the configuration directory and use your environment or secret manager when appropriate. Do not copy credentials into documentation, logs, or MCP conversations.

## Install only the capabilities you need

| Extra | Adds |
| --- | --- |
| `mcp` | The supported MCP SDK and server integration |
| `crypto` | Encryption support for saved credential values |
| `sync` | S3 client (`boto3>=1.40.0`, for conditional operations) and filesystem watcher |
| `all` | MCP, crypto, and sync |
| `dev` | Python tests and S3 mocking |

The documentation website has its own build-only dependency in `website/requirements.txt`; published pages are static HTML.
