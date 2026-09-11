"""CLI for hyperkb knowledge base.

Admin commands (init, config, doctor, reindex, update) for bootstrapping, configuration,
and upgrades. All operational commands (search, add, create, show, list,
check, links) are served exclusively through the MCP server.
"""

import logging
import json
import sqlite3
from dataclasses import fields
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import click

from . import __version__
from .config import KBConfig
from .crypto import is_sensitive_field, mask_value
from .store import KnowledgeStore
from .db import KBDatabase

logger = logging.getLogger("hyperkb.cli")


def _check_local_sync_protocol(config):
    """Warn persistently about blocked remote sync while allowing local work."""
    from .protocol import ProtocolError, ProtocolGate
    try:
        state = ProtocolGate(config).check(allow_local=True)
    except ProtocolError as exc:
        state = exc.as_dict()
    if state.get("blocked") or state.get("status") == "offline":
        click.echo(json.dumps(state, indent=2), err=True)
        click.echo("Local work will continue and pending changes are preserved. Remote sync remains blocked until compatibility is restored.", err=True)
    return state


def _configured_remote(config):
    from .remote import S3Remote
    return S3Remote(
        bucket=config.sync_bucket, prefix=config.sync_prefix,
        region=config.sync_region, endpoint_url=config.sync_endpoint_url,
        access_key=config.sync_access_key, secret_key=config.sync_secret_key,
    )


@click.group()
@click.version_option(version=__version__)
def cli():
    """hkb: A hyperconnected knowledge base with hybrid search.

    \b
    SETUP:
      hkb init                          # Initialize KB at ~/.hkb/
      hkb config rg_weight 0.5         # View/set configuration

    \b
    OPERATIONS (via MCP):
      All knowledge operations (search, add, create, show, list, check,
      links) are handled by the MCP server. Register the
      hkb-mcp server in your Claude Code settings to use them.

    \b
    KB LOCATION:
      KB lives at ~/.hkb/ by default. Use --path to override.

    \b
    DEPENDENCIES:
      Required: Python 3.10+, ripgrep (rg)
      Python packages: click, pyyaml, httpx
      Optional: cryptography (for API key encryption)

    \b
    Install ripgrep: https://github.com/BurntSushi/ripgrep#installation
      brew install ripgrep | apt install ripgrep | cargo install ripgrep
    """
    pass


@cli.command()
@click.option("--path", default=None,
              help="Directory to initialize the knowledge base in. "
                   "If omitted, creates the KB at ~/.hkb/.")
def init(path):
    """Initialize a new knowledge base.

    \b
    Creates:
      - .hkb/config.json  (configuration)
      - .hkb/index.db     (SQLite database with FTS5 indexes)

    \b
    Examples:
      hkb init                                # KB at ~/.hkb/
      hkb init --path ~/alt-kb                # KB at specific path
    """
    if path is None:
        # Global KB
        root = Path.home()
    else:
        root = Path(path).resolve()

    try:
        config = KBConfig.load(str(root))
    except FileNotFoundError:
        config = KBConfig(root=str(root))
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    _check_local_sync_protocol(config)
    store = KnowledgeStore(config)
    try:
        click.echo(store.init())
    finally:
        store.close()


@cli.command()
@click.argument("key")
@click.argument("value", required=False)
@click.option("--set", "use_set", is_flag=True,
              help="Set a sensitive field (e.g. sync_access_key) via hidden interactive prompt. "
                   "Avoids exposing the value in shell history.")
@click.option("--path", default=None, help="KB root directory (default: ~/.hkb/).")
def config(key, value, use_set, path):
    """View or set configuration values.

    \b
    VIEW:  hkb config rg_weight
    SET:   hkb config rg_weight 0.5
           hkb config bm25_weight 0.5
           hkb config route_confidence_threshold 0.6
           hkb config sync_access_key --set

    \b
    SENSITIVE FIELDS:
      sync_access_key and sync_secret_key are displayed masked and stored encrypted.
      Use --set to enter the value via hidden prompt (avoids shell history).
    """
    # Handle --set for sensitive fields: prompt with hidden input
    if use_set and is_sensitive_field(key):
        value = click.prompt("Value", hide_input=True)
    elif use_set:
        # --set on non-sensitive field: if value was given, use it as-is
        if value is None:
            click.echo(f"Error: --set is only needed for sensitive fields.", err=True)
            sys.exit(1)

    # Warn if sensitive field value passed as CLI argument
    if value is not None and is_sensitive_field(key) and not use_set:
        click.echo(
            "Warning: Sensitive value passed as argument is visible in shell history. "
            f"Use 'hkb config {key} --set' instead.",
            err=True,
        )

    if value is None:
        # View mode
        try:
            cfg = KBConfig.load(path)
        except (FileNotFoundError, ValueError) as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

        if key not in {f.name for f in fields(cfg)}:
            click.echo(f"Unknown config key: {key}", err=True)
            sys.exit(1)

        val = getattr(cfg, key)
        if is_sensitive_field(key):
            click.echo(f"{key} = {mask_value(str(val))}")
        else:
            click.echo(f"{key} = {val}")
    else:
        # Set mode
        try:
            cfg = KBConfig.load(path)
        except (FileNotFoundError, ValueError) as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

        if key not in {f.name for f in fields(cfg)}:
            click.echo(f"Unknown config key: {key}", err=True)
            sys.exit(1)

        try:
            value = cfg.set_value(key, value)
        except (ValueError, OSError) as e:
            raise click.ClickException(str(e)) from e

        if is_sensitive_field(key):
            click.echo(f"{key} = {mask_value(str(value))}")
        else:
            click.echo(f"{key} = {value}")


class _ReadOnlyDatabase(KBDatabase):
    """Health-query adapter that never migrates, creates or checkpoints an index."""

    def connect(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path.as_uri() + "?mode=ro", uri=True)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA query_only=ON")
            self.conn.execute("BEGIN")
        return self.conn

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def _check_doctor_schema(conn):
    required = {
        "files": {"name", "path", "description", "keywords", "links", "created_at", "compacted_at"},
        "entries": {"file_name", "epoch", "content", "status", "entry_type", "tags", "weight", "author", "hostname"},
        "entry_links": {"source_file", "source_epoch", "target_file", "target_epoch", "link_type"},
        "entries_fts": {"content", "file_name"},
        "files_fts": {"name", "description", "keywords"},
    }
    missing = []
    for table, columns in required.items():
        present = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if not present:
            missing.append(table)
        else:
            missing.extend(f"{table}.{column}" for column in sorted(columns - present))
    if missing:
        raise click.ClickException(
            "Index schema migration needed (missing " + ", ".join(missing) +
            "). Run hkb reindex with the same --path; doctor left the index unchanged."
        )


@cli.command()
@click.option("--path", default=None, help="KB root directory (default: ~/.hkb/).")
def doctor(path):
    """Check configuration, SQLite integrity and index health without MCP."""
    store = None
    try:
        cfg = KBConfig.load(path)
        if not cfg.db_path.exists():
            raise click.ClickException("Index is missing. Run hkb reindex with the same --path.")
        store = KnowledgeStore(cfg)
        store.db = _ReadOnlyDatabase(cfg)
        conn = store.db.connect()
        check = [tuple(row) for row in conn.execute("PRAGMA quick_check")]
        if check != [("ok",)]:
            raise click.ClickException(f"Index integrity failed: {check}. Run hkb reindex.")
        _check_doctor_schema(conn)
        click.echo(json.dumps(store.health_check(include_tier3=False), indent=2))
    except (OSError, ValueError, sqlite3.Error) as e:
        raise click.ClickException(f"{e}. Run hkb reindex with the same --path to rebuild the index.") from e
    finally:
        if store:
            store.close()


@cli.command()
@click.option("--path", default=None, help="KB root directory (default: ~/.hkb/).")
def reindex(path):
    """Rebuild the index from markdown, preserving configuration and corrupt DBs."""
    store = None
    try:
        config = KBConfig.load(path)
        _check_local_sync_protocol(config)
        store = KnowledgeStore(config)
        recovered = store.recover_index()
        click.echo(recovered or store.reindex())
    except (OSError, ValueError, sqlite3.Error) as e:
        raise click.ClickException(str(e)) from e
    finally:
        if store:
            store.close()


def _find_repo_dir(config_repo: str = "") -> Path | None:
    """Find the hyperkb git repo directory.

    Uses config.update_repo if set, otherwise walks up from this file's
    location to find a .git directory.
    """
    if config_repo:
        p = Path(config_repo)
        if (p / ".git").exists():
            return p
        return None
    # Walk up from package directory
    pkg_dir = Path(__file__).resolve().parent
    for parent in [pkg_dir] + list(pkg_dir.parents):
        if (parent / ".git").exists():
            return parent
    return None


def _git_run(repo: Path, *args, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a git command in the given repo."""
    return subprocess.run(
        ["git", *args],
        cwd=repo, capture_output=True, text=True, timeout=timeout,
    )


def _update_revision(repo: Path, ref: str) -> str:
    result = _git_run(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if result.returncode != 0 or not result.stdout.strip():
        raise click.ClickException(f"Could not resolve {ref}: {result.stderr.strip()}")
    return result.stdout.strip()


def _get_local_tag(repo: Path) -> str:
    """Get the latest local tag via git describe."""
    r = _git_run(repo, "describe", "--tags", "--abbrev=0")
    return r.stdout.strip() if r.returncode == 0 else ""


def _get_remote_tag(repo: Path) -> str:
    """Get the latest tag on origin/main."""
    r = _git_run(repo, "describe", "--tags", "--abbrev=0", "origin/main")
    return r.stdout.strip() if r.returncode == 0 else ""


def _update_log_path() -> Path:
    return Path.home() / ".hkb" / "update.log"


def _log_update(message: str):
    """Append a timestamped line to the update log."""
    log_path = _update_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with open(log_path, "a") as f:
        f.write(f"[{ts}] {message}\n")


@cli.group()
def update():
    """Check for and apply hyperkb updates.

    \b
    COMMANDS:
      hkb update check   Check for available updates
      hkb update apply   Pull latest and reinstall if needed
      hkb update log     Show recent update history
    """
    pass


@update.command("check")
def update_check():
    """Fetch origin and compare source commits as well as release tags."""
    repo = _find_repo_dir()
    if not repo:
        click.echo("Error: Could not find hyperkb git repository.", err=True)
        sys.exit(1)

    click.echo(f"Current version: {__version__}")
    click.echo(f"Repository: {repo}")

    click.echo("Fetching tags from origin...")
    r = _git_run(repo, "fetch", "--tags", "origin")
    if r.returncode != 0:
        click.echo(f"Error: git fetch failed: {r.stderr.strip()}", err=True)
        sys.exit(1)

    local_tag = _get_local_tag(repo)
    remote_tag = _get_remote_tag(repo)

    if not local_tag:
        click.echo("No local tags found.")
    else:
        click.echo(f"Local tag:  {local_tag}")

    if not remote_tag:
        click.echo("No remote tags found.")
    else:
        click.echo(f"Remote tag: {remote_tag}")

    local_head = _update_revision(repo, "HEAD")
    remote_head = _update_revision(repo, "origin/main")
    if local_head == remote_head:
        click.echo(f"\nSource is up to date ({local_head[:8]}).")
        click.echo("Run 'hkb update apply' to refresh installed dependencies if needed.")
    elif _git_run(repo, "merge-base", "--is-ancestor", local_head, remote_head).returncode == 0:
        click.echo(f"\nSource update available: {local_head[:8]} → {remote_head[:8]}")
        click.echo("Run 'hkb update apply' to upgrade, even if the release tag is unchanged.")
    else:
        click.echo("\nLocal source is ahead of or diverged from origin/main; reconcile branches before updating.")


@update.command("apply")
def update_apply():
    """Fast-forward source, reinstall its dependencies, and show restart instructions."""
    repo = _find_repo_dir()
    if not repo:
        click.echo("Error: Could not find hyperkb git repository.", err=True)
        sys.exit(1)

    click.echo(f"Repository: {repo}")

    # Step 1: Fetch
    click.echo("Fetching from origin...")
    r = _git_run(repo, "fetch", "origin", "--tags")
    if r.returncode != 0:
        click.echo(f"Error: git fetch failed: {r.stderr.strip()}", err=True)
        sys.exit(1)

    installed_version = f"v{__version__}" if not __version__.startswith("v") else __version__
    r = _git_run(repo, "status", "--porcelain")
    if r.returncode != 0:
        raise click.ClickException(f"Could not check working tree: {r.stderr.strip()}")
    if r.stdout.strip():
        raise click.ClickException("Working tree has uncommitted changes. Commit or stash first.")

    old_head = _update_revision(repo, "HEAD")
    target_head = _update_revision(repo, "origin/main")
    if old_head != target_head:
        ancestor = _git_run(repo, "merge-base", "--is-ancestor", old_head, target_head)
        if ancestor.returncode != 0:
            raise click.ClickException(
                "Local source is ahead of or diverged from origin/main; refusing to replace it. "
                "Reconcile the branches manually, then run hkb update apply again."
            )
        click.echo("Fast-forwarding to the fetched origin/main revision...")
        r = _git_run(repo, "merge", "--ff-only", target_head)
        if r.returncode != 0:
            raise click.ClickException(f"Fast-forward failed: {r.stderr.strip()}")
        click.echo(f"Updated source: {old_head[:8]} → {target_head[:8]}")
    else:
        click.echo("Source already matches origin/main; refreshing the installed package.")
    new_head = _update_revision(repo, "HEAD")

    # Reinstall even at the same commit: dependencies/installed metadata may lag
    # the checked-out source, including commits published under an unchanged tag.
    click.echo("Reinstalling current source and dependencies...")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", ".[all]"],
            cwd=repo, check=True, timeout=120,
        )
        click.echo("Reinstall complete.")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        raise click.ClickException(
            "Source update completed, but package installation failed. "
            "Resolve the installation error and run hkb update apply again."
        ) from exc

    # Step 6: Copy skill files if present
    skill_mappings = [
        (repo / "SKILL.md", Path.home() / ".claude" / "skills" / "hyperkb" / "SKILL.md"),
        (repo / ".claude" / "skills" / "rem" / "SKILL.md",
         Path.home() / ".claude" / "skills" / "rem" / "SKILL.md"),
    ]
    for src, dst in skill_mappings:
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(src.read_text())
            click.echo(f"Copied skill: {dst}")

    # Each MCP client owns its stdio process; clients restart their own server.
    click.echo("Restart your connected MCP clients to load the updated version.")

    # Step 8: Log the update
    # The nearest release tag may predate this source revision. Report the
    # installed commit rather than labeling an untagged update as an old release.
    msg = f"Installed source at {new_head[:8]} (previous package {installed_version})"
    _log_update(msg)
    click.echo(f"\nDone. {msg}")


@update.command("log")
def update_log():
    """Show the last 20 lines of update history."""
    log_path = _update_log_path()
    if not log_path.exists():
        click.echo("No update history.")
        return
    lines = log_path.read_text().splitlines()
    for line in lines[-20:]:
        click.echo(line)


@cli.group()
def sync():
    """Multi-machine sync via S3.

    \b
    COMMANDS:
      hkb sync setup    Interactive S3 configuration wizard
      hkb sync status   Show protocol compatibility and local sync state
      hkb sync upgrade-protocol --dry-run   Preview the protocol migration
      hkb sync upgrade-protocol   Migrate after upgrading or stopping legacy clients

    Local work continues when protocols differ; only remote sync is blocked.
    """
    pass


@sync.command()
@click.option("--path", default=None, help="KB root directory (default: ~/.hkb/).")
def setup(path):
    """Interactive S3 sync configuration wizard.

    \b
    Configures:
      - S3 bucket and prefix
      - AWS region and optional endpoint URL (for MinIO)
      - Credentials (access key + secret key)
      - Sync interval
      - Initializes git repo in storage directory

    \b
    Examples:
      hkb sync setup
      hkb sync setup --path ~/alt-kb
    """
    try:
        cfg = KBConfig.load(path)
    except (FileNotFoundError, ValueError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    click.echo("=== hyperkb Sync Setup ===\n")

    # Bucket
    bucket = click.prompt(
        "S3 bucket name",
        default=cfg.sync_bucket or "",
    )
    if not bucket:
        click.echo("Error: bucket name is required.", err=True)
        sys.exit(1)

    # Prefix
    prefix = click.prompt(
        "S3 key prefix",
        default=cfg.sync_prefix or "hkb/",
    )

    # Region
    region = click.prompt(
        "AWS region (leave empty for default)",
        default=cfg.sync_region or "",
    )

    # Endpoint URL (for MinIO/custom S3)
    endpoint_url = click.prompt(
        "Custom endpoint URL (for MinIO, leave empty for AWS)",
        default=cfg.sync_endpoint_url or "",
    )

    # Credentials
    access_key = click.prompt(
        "Access key ID",
        default="",
        hide_input=False,
    )
    secret_key = ""
    if access_key:
        secret_key = click.prompt("Secret access key", hide_input=True)

    # Interval
    interval = click.prompt(
        "Sync interval (seconds)",
        default=cfg.sync_interval,
        type=int,
    )

    # Apply config
    cfg.sync_enabled = True
    cfg.sync_bucket = bucket
    cfg.sync_prefix = prefix
    cfg.sync_region = region
    cfg.sync_endpoint_url = endpoint_url
    if access_key:
        cfg.sync_access_key = access_key
    if secret_key:
        cfg.sync_secret_key = secret_key
    cfg.sync_interval = interval
    cfg.save()

    click.echo(f"\nSync configured: s3://{bucket}/{prefix}")

    # Local Git history preserves pending work even while remote sync is blocked.
    _check_local_sync_protocol(cfg)
    try:
        from .sync import GitRepo
        git = GitRepo(cfg.storage_dir)
        git.init()
        click.echo("Git repo initialized in storage directory.")
    except Exception as e:
        click.echo(f"Warning: git init failed: {e}", err=True)

    click.echo("\nSync is configured. Local work is preserved; remote sync requires matching protocol versions.")


@sync.command("status")
@click.option("--path", default=None, help="KB root directory (default: ~/.hkb/).")
def sync_status(path):
    """Show sync state — last sync, pending changes, configuration.

    \b
    Examples:
      hkb sync status
      hkb sync status --path ~/alt-kb
    """
    try:
        cfg = KBConfig.load(path)
    except (FileNotFoundError, ValueError) as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    if not cfg.sync_enabled:
        click.echo("Sync is not enabled. Run 'hkb sync setup' to configure.")
        return

    click.echo(f"Sync: enabled")
    click.echo(f"Bucket: s3://{cfg.sync_bucket}/{cfg.sync_prefix}")
    if cfg.sync_region:
        click.echo(f"Region: {cfg.sync_region}")
    if cfg.sync_endpoint_url:
        click.echo(f"Endpoint: {cfg.sync_endpoint_url}")
    click.echo(f"Interval: {cfg.sync_interval}s")

    from .protocol import ProtocolError, ProtocolGate
    protocol_error = None
    try:
        click.echo(json.dumps(ProtocolGate(cfg).check(), indent=2))
    except ProtocolError as exc:
        protocol_error = exc
        click.echo(json.dumps(exc.as_dict(), indent=2), err=True)

    # Check git status
    try:
        from .sync import GitRepo
        git = GitRepo(cfg.storage_dir)
        if git.is_initialized():
            changed = git.get_changed_files()
            md_changed = [f for f in changed if f.endswith(".md")]
            commits = git.get_commit_count_since_sync()
            click.echo(f"Git: initialized ({commits} commits since last sync)")
            if md_changed:
                click.echo(f"Pending local changes: {len(md_changed)} files")
                for f in md_changed[:10]:
                    click.echo(f"  - {f}")
                if len(md_changed) > 10:
                    click.echo(f"  ... and {len(md_changed) - 10} more")
            else:
                click.echo("No pending local changes.")
        else:
            click.echo("Git: not initialized (run 'hkb sync setup')")
    except Exception as e:
        click.echo(f"Git status error: {e}", err=True)

    if protocol_error is not None:
        raise click.exceptions.Exit(1)


@sync.command("upgrade-protocol")
@click.option("--dry-run", is_flag=True, help="Preview the migration without changing S3 objects.")
@click.option("--path", default=None, help="KB root directory (default: ~/.hkb/).")
def upgrade_protocol(path, dry_run):
    """Migrate an older S3 protocol after upgrading and stopping legacy clients.

    Use --dry-run to review the transformation first. Applying it changes the
    remote compatibility marker under its lease while preserving markdown.
    Upgrade all clients first; legacy clients without protocol checks must stay
    stopped during and after migration. Local pending changes remain on disk.
    After migration, review deltas with hkb_sync(dry_run=True) before syncing.
    """
    from .protocol import ProtocolError
    try:
        cfg = KBConfig.load(path)
        if not cfg.sync_bucket:
            raise click.ClickException("Configure an S3 sync bucket before upgrading its protocol.")
        if not dry_run:
            click.echo("Migrating the S3 protocol. All legacy clients must already be upgraded or stopped.")
        result = _configured_remote(cfg).upgrade_protocol(dry_run=dry_run)
        click.echo(json.dumps(result, indent=2))
        if not dry_run:
            click.echo("Local pending changes are preserved. Review deltas with hkb_sync(dry_run=True) before syncing.")
    except ProtocolError as exc:
        click.echo(json.dumps(exc.as_dict(), indent=2), err=True)
        raise click.exceptions.Exit(1) from exc
    except (OSError, ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    cli()
