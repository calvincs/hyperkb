"""CLI for hyperkb knowledge base.

Admin commands (init, config, update) for bootstrapping, configuration,
and upgrades. All operational commands (search, add, create, show, list,
check, links, reindex) are served exclusively through the MCP server.
"""

import logging
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import click

from . import __version__
from .config import KBConfig
from .crypto import is_sensitive_field, mask_value
from .store import KnowledgeStore

logger = logging.getLogger("hyperkb.cli")


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
      links, reindex) are handled by the MCP server. Register the
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

    config = KBConfig(root=str(root))
    store = KnowledgeStore(config)
    result = store.init()
    store.close()
    click.echo(result)


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
        except FileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

        if not hasattr(cfg, key):
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
        except FileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

        if not hasattr(cfg, key):
            click.echo(f"Unknown config key: {key}", err=True)
            sys.exit(1)

        current = getattr(cfg, key)
        if isinstance(current, float):
            value = float(value)
        elif isinstance(current, int):
            value = int(value)

        setattr(cfg, key, value)
        cfg.save()

        if is_sensitive_field(key):
            click.echo(f"{key} = {mask_value(str(value))}")
        else:
            click.echo(f"{key} = {value}")


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
    """Fetch tags from origin and compare local vs remote versions."""
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

    installed_version = f"v{__version__}" if not __version__.startswith("v") else __version__
    if remote_tag and installed_version != remote_tag:
        click.echo(f"\nUpdate available: {installed_version} → {remote_tag}")
        click.echo("Run 'hkb update apply' to upgrade.")
    elif remote_tag:
        click.echo("\nUp to date.")
    else:
        click.echo("\nCould not determine update status.")


@update.command("apply")
def update_apply():
    """Pull latest changes, reinstall if needed, restart MCP server."""
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

    # Step 2: Compare installed version against latest remote tag
    local_tag = _get_local_tag(repo)
    remote_tag = _get_remote_tag(repo)
    installed_version = f"v{__version__}" if not __version__.startswith("v") else __version__
    if remote_tag and installed_version == remote_tag:
        click.echo(f"Already up to date ({installed_version}).")
        return

    # Step 3: Guard against dirty tree
    r = _git_run(repo, "status", "--porcelain")
    if r.stdout.strip():
        click.echo("Error: Working tree has uncommitted changes. Commit or stash first.", err=True)
        sys.exit(1)

    # Step 4: Capture old HEAD and pull
    old_head = _git_run(repo, "rev-parse", "HEAD").stdout.strip()
    click.echo("Pulling latest changes...")
    r = _git_run(repo, "pull", "--ff-only", "origin", "main")
    if r.returncode != 0:
        click.echo(f"Error: git pull failed: {r.stderr.strip()}", err=True)
        click.echo("Try resolving manually, then run 'hkb update apply' again.")
        sys.exit(1)
    new_head = _git_run(repo, "rev-parse", "HEAD").stdout.strip()

    if old_head == new_head:
        click.echo("No new commits.")
        return

    click.echo(f"Updated: {old_head[:8]} → {new_head[:8]}")

    # Step 5: Always reinstall so setuptools-scm picks up the new tag
    click.echo("Reinstalling (setuptools-scm version sync)...")
    pip_exe = Path(sys.prefix) / "bin" / "pip"
    if not pip_exe.exists():
        pip_exe = Path(sys.executable).parent / "pip"
    try:
        subprocess.run(
            [str(pip_exe), "install", "-e", ".[all]"],
            cwd=repo, check=True, timeout=120,
        )
        click.echo("Reinstall complete.")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        click.echo(f"Warning: pip install failed: {e}", err=True)
        click.echo("You may need to run: pip install -e '.[all]' manually.")

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

    # Step 7: Restart MCP server
    lock_path = Path.home() / ".hkb" / "server.lock"
    if lock_path.exists():
        try:
            pid = int(lock_path.read_text().strip())
            os.kill(pid, signal.SIGTERM)
            click.echo(f"Sent SIGTERM to MCP server (PID {pid}).")
            click.echo("The server will restart automatically when next needed.")
        except (ValueError, ProcessLookupError, PermissionError):
            click.echo("MCP server not running or PID stale — skipping restart.")
    else:
        click.echo("No MCP server lock found — skipping restart.")

    # Step 8: Log the update
    new_tag = _get_local_tag(repo) or new_head[:8]
    msg = f"Updated {installed_version} → {new_tag}"
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
      hkb sync status   Show sync state (last sync, pending changes)
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
    except FileNotFoundError as e:
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

    # Initialize git repo in storage dir
    try:
        from .sync import GitRepo
        git = GitRepo(cfg.storage_dir)
        git.init()
        click.echo("Git repo initialized in storage directory.")
    except Exception as e:
        click.echo(f"Warning: git init failed: {e}", err=True)

    click.echo("\nSync is now enabled. The MCP server will start syncing on next launch.")


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
    except FileNotFoundError as e:
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


if __name__ == "__main__":
    cli()
