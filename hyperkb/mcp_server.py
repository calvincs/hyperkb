"""MCP server for hyperkb — exposes KnowledgeStore as MCP tools over stdio.

Requires: pip install hyperkb[mcp]

Usage:
    hkb-mcp                          # Uses global KB at ~/.hkb/
    hkb-mcp --path /some/dir         # Explicit KB root

Register in Claude Code settings:
    {
        "mcpServers": {
            "hyperkb": {
                "command": "hkb-mcp",
                "args": []
            }
        }
    }
"""

import argparse
import json
import logging
import os
import signal
import sqlite3
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from mcp.server.fastmcp import Context, FastMCP

from . import __version__
from .config import KBConfig
from .format import parse_add_result, parse_time_input, safe_parse_json_list, is_archive_file
from .store import KnowledgeStore


@dataclass
class AppContext:
    """Lifespan context holding the initialized KnowledgeStore."""
    store: KnowledgeStore
    health: dict | None = None
    sync_worker: object | None = None  # SyncWorker instance (if sync enabled)
    update_available: str = ""  # e.g. "v0.1.0 → v0.2.0" if newer version exists
    anchors: list = None  # Session anchor topics
    anchor_files: dict = None  # {file_name: max_score} for anchor boost

    def __post_init__(self):
        if self.anchors is None:
            self.anchors = []
        if self.anchor_files is None:
            self.anchor_files = {}


# ---------------------------------------------------------------------------
# Process-level exclusive lock
# ---------------------------------------------------------------------------

LOCK_FILENAME = "server.lock"

_SIGTERM_TIMEOUT = 5   # seconds to wait after SIGTERM
_SIGKILL_TIMEOUT = 2   # seconds to wait after SIGKILL

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False


class _ServerLock:
    """Prevents multiple hkb-mcp processes from sharing the same KB.

    Uses ``fcntl.flock(LOCK_EX | LOCK_NB)`` on ``<hkb_dir>/server.lock``.
    The kernel auto-releases the lock when the process exits (including
    SIGKILL / os._exit()), so there is no stale-lock problem under normal
    circumstances.

    However, when a client (e.g. Claude Code) reconnects, it may spawn a
    new server without killing the old one.  The old process stays alive
    with a dead stdio pipe, holding the flock.  To recover, ``acquire()``
    will detect the stale holder, terminate it gracefully, and retry.

    On platforms without ``fcntl`` (Windows) the lock is silently skipped.
    """

    def __init__(self, hkb_dir: Path):
        self._path = hkb_dir / LOCK_FILENAME
        self._fd: int | None = None

    # -- helpers --

    @staticmethod
    def _read_pid(fd: int) -> str:
        """Read PID string from an open lock file (best-effort)."""
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            data = os.read(fd, 64)
            return data.decode("ascii", errors="replace").strip()
        except OSError:
            return "unknown"

    @staticmethod
    def _is_hkb_mcp_process(pid: int) -> bool:
        """Check whether *pid* is an hkb-mcp server process.

        On Linux, reads ``/proc/<pid>/cmdline``.  Falls back to a simple
        liveness check (``os.kill(pid, 0)``) on other platforms.
        """
        try:
            cmdline_path = Path(f"/proc/{pid}/cmdline")
            if cmdline_path.exists():
                raw = cmdline_path.read_bytes()
                text = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace")
                return "hkb-mcp" in text or "mcp_server" in text
            # Non-Linux: just check if the process is alive.
            os.kill(pid, 0)
            return True
        except (PermissionError, ProcessLookupError, OSError):
            return False

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        """Return True if *pid* is alive (not dead and not zombie)."""
        try:
            os.waitpid(pid, os.WNOHANG)  # reap zombie if it's our child
        except ChildProcessError:
            pass  # not our child — that's fine
        except OSError:
            pass
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True  # can't signal but it exists

    @staticmethod
    def _terminate_stale(pid: int) -> bool:
        """Try to terminate *pid* gracefully (SIGTERM), escalate to SIGKILL.

        Returns True if the process is confirmed dead.
        """
        if not _ServerLock._pid_alive(pid):
            return True

        # SIGTERM
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False

        deadline = time.monotonic() + _SIGTERM_TIMEOUT
        while time.monotonic() < deadline:
            if not _ServerLock._pid_alive(pid):
                return True
            time.sleep(0.1)

        # SIGKILL
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False

        deadline = time.monotonic() + _SIGKILL_TIMEOUT
        while time.monotonic() < deadline:
            if not _ServerLock._pid_alive(pid):
                return True
            time.sleep(0.1)

        return False

    # -- public API --

    def acquire(self) -> None:
        if not _HAS_FCNTL:
            logger.warning(
                "fcntl unavailable (Windows?) — skipping server lock. "
                "Running multiple hkb-mcp instances may corrupt the index."
            )
            return

        fd = os.open(str(self._path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            existing_pid_str = self._read_pid(fd)
            os.close(fd)

            # --- Stale process recovery ---
            try:
                existing_pid = int(existing_pid_str)
            except (ValueError, TypeError):
                raise RuntimeError(
                    f"Lock held by unparseable PID ({existing_pid_str!r}). "
                    f"Remove {self._path} manually."
                )

            if existing_pid == os.getpid():
                raise RuntimeError(
                    "Lock is already held by this process — double acquire?"
                )

            if self._is_hkb_mcp_process(existing_pid):
                logger.info(
                    "Stale hkb-mcp process detected (PID %d) — terminating",
                    existing_pid,
                )
                if not self._terminate_stale(existing_pid):
                    raise RuntimeError(
                        f"Could not terminate stale hkb-mcp (PID {existing_pid}). "
                        f"Kill it manually: kill -9 {existing_pid}"
                    )
            else:
                logger.debug(
                    "Lock holder PID %d is not hkb-mcp (dead or recycled)",
                    existing_pid,
                )

            # Retry once — the flock should now be free.
            fd = os.open(str(self._path), os.O_RDWR | os.O_CREAT, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                raise RuntimeError(
                    f"Stale recovery failed — lock still held after "
                    f"terminating PID {existing_pid_str}. "
                    f"Remove {self._path} manually."
                )

        # Prevent child processes from inheriting the lock fd.
        try:
            import fcntl as _fcntl
            flags = _fcntl.fcntl(fd, _fcntl.F_GETFD)
            _fcntl.fcntl(fd, _fcntl.F_SETFD, flags | _fcntl.FD_CLOEXEC)
        except OSError:
            pass

        # Write our PID so a blocked instance can report it.
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, str(os.getpid()).encode("ascii"))

        self._fd = fd

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None


# Parse CLI args before server creation so lifespan can use them.
_server_args: argparse.Namespace | None = None


def _parse_server_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="hkb-mcp",
        description="hyperkb MCP server",
    )
    parser.add_argument(
        "--path",
        default=None,
        help="Explicit KB root directory (default: ~/.hkb/).",
    )
    return parser.parse_args(argv)


logger = logging.getLogger("hyperkb.mcp")


def _start_sync_worker(config, store):
    """Start background sync worker if sync is enabled. Returns worker or None."""
    if not config.sync_enabled or not config.sync_bucket:
        return None
    try:
        from .sync import SyncEngine, SyncWorker
        from .remote import S3Remote
        remote = S3Remote(
            bucket=config.sync_bucket,
            prefix=config.sync_prefix,
            region=config.sync_region,
            endpoint_url=config.sync_endpoint_url,
            access_key=config.sync_access_key,
            secret_key=config.sync_secret_key,
        )
        engine = SyncEngine(
            storage_dir=config.storage_dir,
            remote=remote,
            config=config,
            reindex_fn=store.reindex,
        )
        engine.setup()
        worker = SyncWorker(engine, interval=config.sync_interval)
        worker.start()
        logger.info("Sync worker started (interval=%ds)", config.sync_interval)
        return worker
    except Exception as e:
        logger.warning("Failed to start sync worker: %s", e)
        return None


def _check_for_update() -> str:
    """Non-blocking check for newer git tags. Returns 'vOLD → vNEW' or empty string."""
    import subprocess
    try:
        # Find the repo root from this file's location
        pkg_dir = Path(__file__).resolve().parent
        repo_dir = None
        for parent in [pkg_dir] + list(pkg_dir.parents):
            if (parent / ".git").exists():
                repo_dir = parent
                break
        if not repo_dir:
            return ""

        # Fetch tags with a short timeout
        subprocess.run(
            ["git", "fetch", "--tags", "origin"],
            cwd=repo_dir, capture_output=True, timeout=3,
        )
        # Local latest tag
        local = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            cwd=repo_dir, capture_output=True, text=True, timeout=3,
        )
        # Remote latest tag on main
        remote = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0", "origin/main"],
            cwd=repo_dir, capture_output=True, text=True, timeout=3,
        )
        local_tag = local.stdout.strip()
        remote_tag = remote.stdout.strip()
        if local_tag and remote_tag and local_tag != remote_tag:
            return f"{local_tag} → {remote_tag}"
    except Exception:
        pass
    return ""


_DB_CONNECT_RETRIES = 5
_DB_CONNECT_BACKOFF = 2  # seconds, doubles each retry


def _connect_with_retry(store: "KnowledgeStore") -> None:
    """Connect to the DB, retrying on 'database is locked'.

    When the MCP server is restarted (e.g. during development), the previous
    process may not have fully released its SQLite locks.  Retrying with
    exponential backoff lets us wait for the old process to die.
    """
    delay = _DB_CONNECT_BACKOFF
    for attempt in range(1, _DB_CONNECT_RETRIES + 1):
        try:
            store.db.connect()
            store.sync_entry_links()
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or attempt == _DB_CONNECT_RETRIES:
                raise
            logger.warning(
                "DB locked on startup (attempt %d/%d), retrying in %ds…",
                attempt, _DB_CONNECT_RETRIES, delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Initialize KnowledgeStore on startup, close on shutdown.

    Auto-creates the KB with sensible defaults if it doesn't exist yet,
    so the MCP server "just works" on first launch without requiring
    a prior ``hkb init``.

    Acquires a process-level exclusive lock (``server.lock``) to prevent
    multiple hkb-mcp instances from sharing the same KB simultaneously.
    """
    args = _server_args or _parse_server_args([])

    # Determine hkb_dir and acquire process lock BEFORE config load.
    if args.path is not None:
        hkb_dir = Path(args.path).resolve() / ".hkb"
    else:
        hkb_dir = Path.home() / ".hkb"
    hkb_dir.mkdir(parents=True, exist_ok=True)

    lock = _ServerLock(hkb_dir)
    lock.acquire()
    try:
        # Lightweight update check (non-blocking, skips silently on failure)
        update_msg = _check_for_update()

        try:
            config = KBConfig.load(args.path)
        except FileNotFoundError:
            # Auto-init: create KB with embeddings disabled (lightweight default)
            logger.info("KB not found — auto-initializing at %s", args.path or "~/.hkb/")
            if args.path is None:
                root = str(Path.home())
            else:
                root = str(Path(args.path).resolve())
            config = KBConfig(root=root)
            store = KnowledgeStore(config)
            store.init()
            _connect_with_retry(store)
            health = store.health_snapshot()
            worker = _start_sync_worker(config, store)
            try:
                yield AppContext(store=store, health=health, sync_worker=worker,
                                 update_available=update_msg)
            finally:
                if worker:
                    worker.stop()
                store.close()
            return

        store = KnowledgeStore(config)
        _connect_with_retry(store)
        health = store.health_snapshot()
        worker = _start_sync_worker(config, store)
        try:
            yield AppContext(store=store, health=health, sync_worker=worker,
                             update_available=update_msg)
        finally:
            if worker:
                worker.stop()
            store.close()
    finally:
        lock.release()


SERVER_INSTRUCTIONS = """\
hyperkb is your persistent knowledge base — long-term memory that survives across sessions.
10 tools, each with sub-actions where applicable.

START OF SESSION: Call hkb_session(action="briefing") for overview, then hkb_search for targeted lookup.

WHEN TO RECORD (proactively): key findings, bugs/root causes, architecture decisions,
hard-won config values, milestones, procedural skills, resumption context.
DO NOT RECORD: trivial edits, transient state, info already in codebase, raw tracebacks.

ENTRY FORMAT: 2-5 sentences, [[file.name]] wiki-links, @weight: high for important entries.
WORKFLOW: hkb_show() → find file → hkb_add(to=file). No file? hkb_add(create_file=True).

TOOLS:
- hkb_search: Find entries (mode=hybrid/rg/bm25/recent/check)
- hkb_context: Token-budgeted retrieval (mode=packed/suggest/narrative)
- hkb_show: Read files, list files (name="" → list), link graph (links=True)
- hkb_add: Add entry or create file (create_file=True)
- hkb_update: Amend entry (action=update/archive/batch)
- hkb_task: Task lifecycle (action=create/show/update/list)
- hkb_sync: Sync operations (action=push/pull/both/status/config/conflicts)
- hkb_session: Briefing/review/anchor (action=briefing/review/anchor)
- hkb_view: Named file groupings (action=set/list)
- hkb_health: Maintenance (action=check/reindex/compact)
"""

mcp_server = FastMCP(f"hyperkb v{__version__}", instructions=SERVER_INSTRUCTIONS, lifespan=app_lifespan)


def _get_store(ctx: Context) -> KnowledgeStore:
    """Extract the KnowledgeStore from lifespan context."""
    return ctx.request_context.lifespan_context.store


def _parse_time(value: str | None) -> int | None:
    """Parse a time string, returning None for empty/None."""
    if not value:
        return None
    return parse_time_input(value)


def _extract_snippet(content: str, query: str, max_len: int = 200) -> str:
    """Extract a snippet centered on the first query term match."""
    if len(content) <= max_len:
        return content
    query_terms = query.lower().split()
    content_lower = content.lower()
    best_pos = -1
    for term in query_terms:
        pos = content_lower.find(term)
        if pos != -1 and (best_pos == -1 or pos < best_pos):
            best_pos = pos
    if best_pos == -1:
        best_pos = 0
    start = max(0, best_pos - max_len // 4)
    end = min(len(content), start + max_len)
    if end - start < max_len:
        start = max(0, end - max_len)
    snippet = content[start:end]
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet += "..."
    return snippet


def _get_anchor_files(ctx: Context) -> dict:
    """Get anchor_files from session context, or empty dict if no anchors."""
    try:
        return ctx.request_context.lifespan_context.anchor_files or {}
    except Exception:
        return {}


def _apply_anchor_boost(results: list, anchor_files: dict, boost: float = 1.5, key: str = "file_name") -> list:
    """Apply score boost to results matching anchored files. Mutates in-place and returns."""
    if not anchor_files:
        return results
    for r in results:
        fname = r.get(key, "") if isinstance(r, dict) else getattr(r, key, "")
        if fname in anchor_files:
            if isinstance(r, dict):
                r["score"] = r.get("score", 0) * boost
            else:
                r.score = r.score * boost
    return results


# ---------------------------------------------------------------------------
# Tools (10 consolidated tools with sub-actions)
# ---------------------------------------------------------------------------

@mcp_server.tool()
def hkb_search(
    query: str = "",
    mode: str = "hybrid",
    top: int = 10,
    domain: str = "",
    after: str = "",
    before: str = "",
    offset: int = 0,
    status: str = "",
    type: str = "",
    include_archived: bool = False,
    author: str = "",
    hostname: str = "",
    ctx: Context = None,
) -> str:
    """Search entries, get recent timeline, or preview content routing.

    Args:
        query: Search terms (required except mode=recent).
        mode: "hybrid" (rg+bm25, default), "rg" (exact), "bm25" (keyword), "recent" (timeline), "check" (preview routing).
        top: Max results (default 10).
        domain: Namespace prefix filter (e.g. "security").
        after: Time filter start (epoch, duration "2d"/"4h"/"1w", or ISO date).
        before: Time filter end. Same formats as after.
        offset: Skip N results for pagination.
        status: Filter by status (active, superseded, resolved).
        type: Filter by type (note, finding, decision, task).
        include_archived: Include .archive entries (default False).
        author: Filter by author.
        hostname: Filter by hostname.

    Returns:
        mode=hybrid/rg/bm25: JSON array of {file_name, content, epoch, score, ...}.
        mode=recent: JSON array of {epoch, content, file_name, type, ...}.
        mode=check: JSON array of routing candidates with scores.
    """
    store = _get_store(ctx)
    top = max(1, min(top, 500))
    offset = max(0, offset)
    try:
        after_epoch = _parse_time(after)
        before_epoch = _parse_time(before)

        # mode=check: preview content routing
        if mode == "check":
            if not query:
                return json.dumps({"status": "error", "message": "query is required for mode=check"})
            candidates = store.check_content(query)
            candidates = [c for c in candidates if c.score > 0.01]
            output = [
                {
                    "name": c.name,
                    "score": round(c.score, 4),
                    "description": c.description,
                    "keywords": c.keywords,
                    "reason": c.reason,
                }
                for c in candidates
            ]
            return json.dumps(output, indent=2)

        # mode=recent: timeline
        if mode == "recent":
            entries = store.get_recent(
                after_epoch=after_epoch or 0,
                before_epoch=before_epoch or 0,
                limit=top,
                domain=domain,
                author=author,
                hostname=hostname,
            )
            output = [
                {
                    "epoch": e["epoch"],
                    "content": e["content"],
                    "file_name": e["file_name"],
                    "type": e.get("entry_type", "note"),
                    "tags": e.get("tags", ""),
                    "author": e.get("author", ""),
                    "hostname": e.get("hostname", ""),
                }
                for e in entries
            ]
            return json.dumps(output, indent=2)

        # mode=hybrid/rg/bm25: standard search
        if not query:
            return json.dumps({"status": "error", "message": "query is required for search"})
        results = store.search(
            query=query,
            mode=mode,
            limit=top,
            domain=domain or None,
            after_epoch=after_epoch,
            before_epoch=before_epoch,
            offset=offset,
            status=status or None,
            entry_type=type or None,
            include_archived=include_archived,
            author=author or None,
            hostname=hostname or None,
        )
        anchor_files = _get_anchor_files(ctx)
        _apply_anchor_boost(results, anchor_files)
        results.sort(key=lambda r: r.score, reverse=True)

        output = [
            {
                "file_name": r.file_name,
                "content": r.content,
                "snippet": _extract_snippet(r.content, query),
                "epoch": r.epoch,
                "score": round(r.score, 4),
                "source": r.source,
                "status": r.status,
                "type": r.entry_type,
                "tags": r.tags,
                "weight": r.weight,
                "author": r.author,
                "hostname": r.hostname,
            }
            for r in results
        ]
        return json.dumps(output, indent=2)
    except ValueError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp_server.tool()
def hkb_show(
    name: str = "",
    after: str = "",
    before: str = "",
    last: int = 0,
    compact: bool = False,
    epoch: int = 0,
    links: bool = False,
    domain: str = "",
    sort: str = "name",
    ctx: Context = None,
) -> str:
    """Read file contents, list all files, or show link graph.

    Args:
        name: Dotted filename. Omit to list all files.
        after: Time filter start (epoch, duration, ISO date).
        before: Time filter end.
        last: Only N most recent entries (0 = all).
        compact: Truncate entries to first line.
        epoch: Show entry-level link graph for this epoch (requires links=True or standalone).
        links: Append link graph to file view.
        domain: Filter file listing by prefix (e.g. "security").
        sort: File listing sort — "name" (default) or "recent".

    Returns:
        name="": JSON {scope, files[]} file listing.
        name set: JSON {header, entries[]}. With links=True, adds link_graph.
        epoch>0 + links: entry-level link graph.
    """
    store = _get_store(ctx)
    try:
        # No name → file listing
        if not name:
            files = store.list_files(domain=domain or None)
            if sort == "recent":
                files.sort(key=lambda f: f.get("created_at", ""), reverse=True)
            for f in files:
                for key in ("keywords", "links"):
                    f[key] = safe_parse_json_list(f.get(key, "[]"))
                f["is_archive"] = is_archive_file(f.get("name", ""))
            return json.dumps({"scope": "global", "files": files}, indent=2)

        # Entry-level link graph
        if epoch > 0 and links:
            link_data = store.get_entry_links(name, epoch)
            return json.dumps({
                "entry": {"file": name, "epoch": epoch},
                "outbound": link_data["outbound"],
                "inbound": link_data["inbound"],
            }, indent=2)

        # File view
        last = max(0, min(last, 500))
        after_epoch = _parse_time(after)
        before_epoch = _parse_time(before)
        data = store.show_file(
            name,
            after_epoch=after_epoch,
            before_epoch=before_epoch,
            last_n=last if last > 0 else None,
        )
        if compact:
            for e in data["entries"]:
                e["content"] = e["content"].split("\n", 1)[0]

        # Append link graph if requested
        if links:
            if epoch > 0:
                link_data = store.get_entry_links(name, epoch)
                data["link_graph"] = {
                    "entry": {"file": name, "epoch": epoch},
                    "outbound": link_data["outbound"],
                    "inbound": link_data["inbound"],
                }
            else:
                data["link_graph"] = store.get_links(name)

        return json.dumps(data, indent=2)
    except FileNotFoundError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp_server.tool()
def hkb_add(
    content: str = "",
    to: str = "",
    epoch: int = 0,
    create_file: bool = False,
    description: str = "",
    keywords: list[str] = [],
    file_links: list[str] = [],
    ctx: Context = None,
) -> str:
    """Add an entry or create a new file.

    Record proactively: findings, bugs, decisions, config values, milestones.
    Content: 2-5 sentences, [[file.name]] for cross-refs, one topic per entry.

    Args:
        content: Entry content (required for entries).
        to: Target file. Omit for auto-routing.
        epoch: Override timestamp (0 = now).
        create_file: If True, create a new file instead of adding an entry.
        description: File description (required when create_file=True).
        keywords: File keywords (for create_file).
        file_links: Related files (for create_file).

    Returns:
        Entry: JSON {status, file, epoch}.
        File: JSON {status, message}.
    """
    store = _get_store(ctx)
    try:
        if create_file:
            if not to:
                return json.dumps({"status": "error", "message": "'to' (file name) is required when create_file=True"})
            result = store.create_file(
                name=to,
                description=description,
                keywords=list(keywords),
                links=list(file_links),
            )
            return json.dumps({"status": "ok", "message": result})
        if not content:
            return json.dumps({"status": "error", "message": "content is required"})
        result = store.add_entry(
            content=content,
            file_name=to or None,
            epoch=epoch if epoch > 0 else None,
        )
        return json.dumps(parse_add_result(result))
    except ValueError as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp_server.tool()
def hkb_update(
    file: str = "",
    epoch: int = 0,
    action: str = "update",
    new_content: str = "",
    set_status: str = "",
    add_tags: str = "",
    remove_tags: str = "",
    updates: str = "",
    ctx: Context = None,
) -> str:
    """Amend entry content/metadata, archive entries, or batch update.

    Args:
        file: Dotted filename (required for update/archive).
        epoch: Entry epoch (required for update/archive).
        action: "update" (default), "archive", or "batch".
        new_content: Replace entry prose (action=update).
        set_status: Change status (action=update).
        add_tags: Tags to add (action=update).
        remove_tags: Tags to remove (action=update).
        updates: JSON array for action=batch. Each: {file, epoch, set_status?, add_tags?, remove_tags?}.

    Returns:
        update/archive: JSON {status, file, epoch}.
        batch: JSON {status, updated, failed, results[]}.
    """
    store = _get_store(ctx)
    try:
        if action == "archive":
            if not file or not epoch:
                return json.dumps({"status": "error", "message": "file and epoch required for archive"})
            result = store.archive_entry(file_name=file, epoch=epoch)
            return json.dumps(result)

        if action == "batch":
            return _do_batch_update(store, updates)

        # Default: single update
        if not file or not epoch:
            return json.dumps({"status": "error", "message": "file and epoch required for update"})
        result = store.update_entry(
            file_name=file,
            epoch=epoch,
            new_content=new_content or None,
            set_status=set_status or None,
            add_tags=add_tags or None,
            remove_tags=remove_tags or None,
        )
        return json.dumps(result)
    except ValueError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


def _do_batch_update(store, updates_json: str) -> str:
    """Handle batch update logic."""
    try:
        items = json.loads(updates_json)
    except (json.JSONDecodeError, TypeError) as e:
        return json.dumps({"status": "error", "message": f"Invalid JSON: {e}"})
    if not isinstance(items, list):
        return json.dumps({"status": "error", "message": "Updates must be a JSON array."})
    if len(items) == 0:
        return json.dumps({"status": "error", "message": "Updates array is empty."})
    items = items[:50]
    results = []
    updated = 0
    failed = 0
    for item in items:
        if not isinstance(item, dict):
            results.append({"status": "error", "message": "Item is not an object."})
            failed += 1
            continue
        f = item.get("file")
        ep = item.get("epoch")
        if not f or ep is None:
            results.append({"status": "error", "message": "Missing 'file' or 'epoch'."})
            failed += 1
            continue
        set_status = item.get("set_status")
        add_tags = item.get("add_tags")
        remove_tags = item.get("remove_tags")
        if not any([set_status, add_tags, remove_tags]):
            results.append({"file": f, "epoch": ep, "status": "error", "message": "No changes specified."})
            failed += 1
            continue
        try:
            store.update_entry(
                file_name=f, epoch=ep,
                set_status=set_status or None,
                add_tags=add_tags or None,
                remove_tags=remove_tags or None,
            )
            results.append({"file": f, "epoch": ep, "status": "ok"})
            updated += 1
        except Exception as e:
            results.append({"file": f, "epoch": ep, "status": "error", "message": str(e)})
            failed += 1
    overall = "ok" if failed == 0 else ("partial" if updated > 0 else "error")
    return json.dumps({"status": overall, "updated": updated, "failed": failed, "results": results}, indent=2)


def _parse_duration_seconds(value: str) -> int:
    """Parse a duration string like '4h' or '7d' into seconds."""
    import re as _re
    m = _re.match(r"^(\d+)([mhdw])$", value.strip())
    if m:
        amount = int(m.group(1))
        unit = m.group(2)
        multipliers = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
        return amount * multipliers[unit]
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Cannot parse duration '{value}'. "
            f"Use: seconds (14400), or duration (4h, 7d, 1w)."
        )


@mcp_server.tool()
def hkb_session(
    action: str = "briefing",
    domain: str = "",
    after: str = "",
    before: str = "",
    focus: str = "",
    view: str = "",
    top: int = 10,
    group_by: str = "file",
    topics: str = "",
    clear: bool = False,
    ctx: Context = None,
) -> str:
    """Session management: briefing, review, or anchor.

    Args:
        action: "briefing" (default), "review", or "anchor".
        domain: Namespace prefix filter.
        after: Time filter start.
        before: Time filter end.
        focus: Topic focus for briefing (uses search instead of chronological).
        view: Named view to scope briefing.
        top: Max entries (briefing: recent, review: total).
        group_by: Review grouping — "file", "type", or "status".
        topics: Comma-separated anchor topics (action=anchor).
        clear: Clear all anchors (action=anchor).

    Returns:
        briefing: JSON with summary, recent_activity, open_tasks, files.
        review: JSON with grouped entries, diagnostics, distributions.
        anchor: JSON with active anchors and anchored files.
    """
    store = _get_store(ctx)
    try:
        if action == "anchor":
            return _do_anchor(store, ctx, topics, clear)
        if action == "review":
            return _do_session_review(store, after, before, top, domain, group_by)
        # Default: briefing
        return _do_briefing(store, ctx, domain, after, focus, view, top)
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


def _do_briefing(store, ctx, domain, after, focus, view, top_recent, top_tasks=20):
    """Generate session briefing."""
    top_recent = max(1, min(top_recent, 500))
    top_tasks = max(1, min(top_tasks, 500))
    after_epoch = _parse_time(after) or 0

    view_files = None
    if view:
        view_data = store.get_view(view)
        if view_data:
            view_files = set(view_data["files"])

    if focus:
        search_results = store.search(
            focus, limit=top_recent, domain=domain or None,
            after_epoch=after_epoch if after_epoch else None,
        )
        recent = [
            {
                "epoch": r.epoch, "content": r.content, "file_name": r.file_name,
                "entry_type": r.entry_type or "note", "status": r.status or "active",
                "tags": r.tags or "", "relevance": round(r.score, 4),
            }
            for r in search_results
        ]
        if view_files:
            recent = [e for e in recent if e["file_name"] in view_files]
    else:
        recent = store.get_recent(after_epoch=after_epoch, limit=top_recent, domain=domain)
        if view_files:
            recent = [e for e in recent if e["file_name"] in view_files]

    tasks = store.db.get_tasks(status_filter="pending,in_progress,blocked", domain=domain)[:top_tasks]
    if focus:
        focus_lower = focus.lower()
        tasks = [t for t in tasks if focus_lower in t.get("content", "").lower()
                 or focus_lower in t.get("file_name", "").lower()]
    if view_files:
        tasks = [t for t in tasks if t["file_name"] in view_files]

    all_files = store.list_files(domain=domain or None)
    if view_files:
        all_files = [f for f in all_files if f.get("name") in view_files]

    files_out = []
    total_entries = 0
    for f in all_files:
        if is_archive_file(f.get("name", "")):
            continue
        entry_count = f.get("entry_count", 0) or 0
        total_entries += entry_count
        files_out.append({
            "name": f["name"], "description": f.get("description", ""),
            "entry_count": entry_count, "latest_epoch": f.get("latest_epoch") or 0,
        })
    files_out.sort(key=lambda f: f["latest_epoch"], reverse=True)

    recent_grouped: dict[str, list] = {}
    authors_seen: set[str] = set()
    hostnames_seen: set[str] = set()
    for e in recent:
        preview = e["content"][:150] + ("..." if len(e["content"]) > 150 else "")
        entry_author = e.get("author", "") or ""
        entry_hostname = e.get("hostname", "") or ""
        if entry_author:
            authors_seen.add(entry_author)
        if entry_hostname:
            hostnames_seen.add(entry_hostname)
        item = {
            "epoch": e["epoch"], "preview": preview,
            "type": e.get("entry_type") or "note", "tags": e.get("tags", ""),
            "author": entry_author, "hostname": entry_hostname,
        }
        if "relevance" in e:
            item["relevance"] = e["relevance"]
        recent_grouped.setdefault(e["file_name"], []).append(item)

    tasks_out = [
        {
            "epoch": t["epoch"], "file_name": t["file_name"],
            "preview": t["content"][:150] + ("..." if len(t["content"]) > 150 else ""),
            "status": t.get("status", "pending"), "tags": t.get("tags", ""),
        }
        for t in tasks
    ]

    health_hints = {"issues": 0, "highlights": [], "detail": ""}
    try:
        cached_health = ctx.request_context.lifespan_context.health
        if cached_health:
            summary = cached_health.get("summary", {})
            issue_count = summary.get("total_issues", 0)
            health_hints["issues"] = issue_count
            if issue_count > 0:
                for c in cached_health.get("checks", []):
                    if c["status"] != "ok":
                        health_hints["highlights"].append(c["message"])
                health_hints["detail"] = "Run hkb_health for full diagnostics."
    except Exception:
        pass

    update_note = ""
    try:
        update_msg = ctx.request_context.lifespan_context.update_available
        if update_msg:
            update_note = f"Update available: {update_msg}. Run: hkb update apply"
    except Exception:
        pass

    result = {
        "summary": {
            "total_files": len(files_out), "total_entries": total_entries,
            "recent_entry_count": len(recent), "open_task_count": len(tasks_out),
            "provenance": {"authors": sorted(authors_seen), "hostnames": sorted(hostnames_seen)},
        },
        "health_hints": health_hints, "update_available": update_note,
        "recent_activity": recent_grouped, "open_tasks": tasks_out, "files": files_out,
    }
    if focus:
        result["focus"] = focus
    if view:
        result["view"] = view
    return json.dumps(result, indent=2)


def _do_session_review(store, after, before, top, domain, group_by):
    """Generate session review with diagnostics."""
    top = max(1, min(top, 500))
    if group_by not in ("file", "type", "status"):
        group_by = "file"
    after_epoch = _parse_time(after) or 0
    before_epoch = _parse_time(before) or 0
    entries = store.get_recent(
        after_epoch=after_epoch, before_epoch=before_epoch, limit=top, domain=domain,
    )

    group_key_map = {"file": "file_name", "type": "entry_type", "status": "status"}
    key_field = group_key_map[group_by]
    grouped: dict[str, list] = {}
    for e in entries:
        gk = e.get(key_field) or ("note" if key_field == "entry_type" else "active")
        preview = e["content"][:150] + ("..." if len(e["content"]) > 150 else "")
        item = {
            "epoch": e["epoch"], "preview": preview,
            "status": e.get("status") or "active", "type": e.get("entry_type") or "note",
            "tags": e.get("tags", ""), "file": e["file_name"],
            "author": e.get("author", ""), "hostname": e.get("hostname", ""),
        }
        grouped.setdefault(gk, []).append(item)

    type_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    author_counts: dict[str, int] = {}
    hostname_counts: dict[str, int] = {}
    for e in entries:
        t = e.get("entry_type") or "note"
        s = e.get("status") or "active"
        a = e.get("author") or ""
        h = e.get("hostname") or ""
        type_counts[t] = type_counts.get(t, 0) + 1
        status_counts[s] = status_counts.get(s, 0) + 1
        if a:
            author_counts[a] = author_counts.get(a, 0) + 1
        if h:
            hostname_counts[h] = hostname_counts.get(h, 0) + 1

    now = int(time.time())
    thirty_days = 30 * 86400
    untagged = []
    stale_active = []
    for e in entries:
        if not e.get("tags"):
            untagged.append({"file": e["file_name"], "epoch": e["epoch"]})
        status = e.get("status") or "active"
        if status == "active" and (now - e["epoch"]) > thirty_days:
            stale_active.append({"file": e["file_name"], "epoch": e["epoch"]})

    by_file: dict[str, list] = {}
    for e in entries:
        by_file.setdefault(e["file_name"], []).append(e)
    potential_duplicates = []
    for fname, file_entries in by_file.items():
        seen: dict[str, list[int]] = {}
        for e in file_entries:
            prefix = e["content"][:80].lower().strip()
            seen.setdefault(prefix, []).append(e["epoch"])
        for prefix, epochs in seen.items():
            if len(epochs) > 1:
                potential_duplicates.append({"file": fname, "epochs": epochs, "preview": prefix[:80]})

    all_epochs = [e["epoch"] for e in entries]
    time_range = {"earliest": min(all_epochs) if all_epochs else 0, "latest": max(all_epochs) if all_epochs else 0}

    result = {
        "summary": {"total_entries": len(entries), "groups": len(grouped), "time_range": time_range},
        f"entries_by_{group_by}": grouped,
        "diagnostics": {
            "untagged_count": len(untagged), "untagged": untagged,
            "stale_active_count": len(stale_active), "stale_active": stale_active,
            "potential_duplicates": potential_duplicates,
        },
        "type_distribution": type_counts, "status_distribution": status_counts,
        "author_distribution": author_counts, "hostname_distribution": hostname_counts,
    }
    return json.dumps(result, indent=2)


def _do_anchor(store, ctx, topics, clear):
    """Handle session anchoring."""
    app_ctx = ctx.request_context.lifespan_context
    if clear:
        app_ctx.anchors = []
        app_ctx.anchor_files = {}
        return json.dumps({"status": "ok", "anchors": [], "anchor_files": {}})
    if not topics or not topics.strip():
        return json.dumps({
            "status": "ok",
            "anchors": app_ctx.anchors,
            "anchor_files": {k: round(v, 4) for k, v in app_ctx.anchor_files.items()},
        })
    topic_list = [t.strip() for t in topics.split(",") if t.strip()]
    app_ctx.anchors = topic_list
    anchor_files: dict[str, float] = {}
    for topic in topic_list:
        results = store.search(topic, limit=10)
        for r in results:
            if r.file_name not in anchor_files or r.score > anchor_files[r.file_name]:
                anchor_files[r.file_name] = r.score
    app_ctx.anchor_files = anchor_files
    return json.dumps({
        "status": "ok",
        "anchors": topic_list,
        "anchor_files": {k: round(v, 4) for k, v in anchor_files.items()},
    })


# ---------------------------------------------------------------------------
# Health, task, sync, context, view tools
# ---------------------------------------------------------------------------

_HEALTH_CHECK_GROUPS = {
    "links": {"broken_outbound_links", "self_links", "header_link_integrity", "entry_links_coverage", "orphan_entry_links"},
    "sync": {"db_vs_disk_file_count", "disk_db_entry_drift", "entry_links_coverage"},
    "quality": {"stale_active", "untagged_entries", "potential_duplicates", "misplaced_archived", "empty_files", "compaction_readiness"},
}

_MAX_FIX_COMMANDS = 5


def _human_size(nbytes: int) -> str:
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def _collect_system_stats(store, ctx) -> dict:
    """Gather system stats: file/entry counts, disk sizes, sync status."""
    stats: dict = {}
    try:
        stats["total_files"] = len(store.db.list_files())
        stats["total_entries"] = store.db.count_entries()
        stats["total_entry_links"] = store.db.count_entry_links()
    except Exception:
        pass

    # Storage dir size
    try:
        storage_bytes = sum(
            f.stat().st_size for f in store.storage_dir.glob("*.md")
        )
        stats["storage_size_bytes"] = storage_bytes
        stats["storage_size_human"] = _human_size(storage_bytes)
    except Exception:
        stats["storage_size_bytes"] = 0
        stats["storage_size_human"] = "unknown"

    # DB size
    try:
        db_bytes = store.config.db_path.stat().st_size
        stats["db_size_bytes"] = db_bytes
        stats["db_size_human"] = _human_size(db_bytes)
    except Exception:
        stats["db_size_bytes"] = 0
        stats["db_size_human"] = "unknown"

    # Sync status
    sync_info: dict = {"enabled": store.config.sync_enabled}
    if store.config.sync_enabled:
        engine = _get_sync_engine(ctx)
        if engine is not None:
            try:
                s = engine.get_status()
                sync_info["last_sync_time"] = s.get("last_sync_time", 0)
                sync_info["last_sync_status"] = s.get("last_sync_status", "never")
                sync_info["last_sync_error"] = s.get("last_sync_error", "")
                sync_info["local_pending_count"] = s.get("local_pending_count", 0)
                sync_info["commits_since_sync"] = s.get("commits_since_sync", 0)
                sync_info["bucket"] = s.get("bucket", "")
                sync_info["prefix"] = s.get("prefix", "")
            except Exception:
                sync_info["last_sync_status"] = "error"
    stats["sync"] = sync_info
    return stats


def _enrich_fix_hints(checks: list[dict]) -> None:
    """Add actionable fix_commands to each check result."""
    for c in checks:
        if c["status"] == "ok":
            c["fix_commands"] = []
            continue

        name = c["name"]
        details = c.get("details", [])
        cmds: list[str] = []

        if name == "entry_links_coverage":
            cmds.append("hkb_health(fix=True)")
        elif name == "orphan_entry_links":
            cmds.append("hkb_health(fix=True)")
        elif name in ("db_vs_disk_file_count", "disk_db_entry_drift"):
            cmds.append("hkb_reindex()")
        elif name == "empty_files":
            files = details[0].get("files", []) if details else []
            for f in files[:_MAX_FIX_COMMANDS]:
                cmds.append(f'hkb_add(to="{f}", content="...")')
        elif name == "broken_outbound_links":
            for d in details[:_MAX_FIX_COMMANDS]:
                target = d.get("target_file") or d.get("target", "")
                if target:
                    cmds.append(f'hkb_create(name="{target}", description="...", keywords=["..."])')
        elif name == "self_links":
            seen = set()
            for d in details:
                f = d.get("source_file", "")
                if f and f not in seen:
                    cmds.append(f'hkb_show(name="{f}") # review self-referencing wiki-links')
                    seen.add(f)
                if len(cmds) >= _MAX_FIX_COMMANDS:
                    break
        elif name == "misplaced_archived":
            for d in details[:_MAX_FIX_COMMANDS]:
                f = d.get("file_name", "")
                ep = d.get("epoch", 0)
                cmds.append(f'hkb_archive(name="{f}", epoch={ep})')
        elif name == "compaction_readiness":
            for d in details[:_MAX_FIX_COMMANDS]:
                f = d.get("file", "")
                cmds.append(f'hkb_compact(name="{f}")')
        elif name == "stale_active":
            for d in details[:_MAX_FIX_COMMANDS]:
                f = d.get("file", d.get("file_name", ""))
                ep = d.get("epoch", 0)
                cmds.append(f'hkb_update(name="{f}", epoch={ep}, set_status="resolved") # or superseded')
        elif name == "untagged_entries":
            for d in details[:_MAX_FIX_COMMANDS]:
                f = d.get("file", d.get("file_name", ""))
                ep = d.get("epoch", 0)
                cmds.append(f'hkb_update(name="{f}", epoch={ep}, add_tags="...")')
        elif name == "potential_duplicates":
            for d in details[:_MAX_FIX_COMMANDS]:
                f = d.get("file", "")
                epochs = d.get("epochs", [])
                cmds.append(f'hkb_show(name="{f}") # review duplicate epochs: {epochs}')

        overflow = c["count"] - len(cmds)
        if overflow > 0 and cmds:
            cmds.append(f"... and {overflow} more")
        c["fix_commands"] = cmds


@mcp_server.tool()
def hkb_health(
    action: str = "check",
    checks: str = "all",
    fix: bool = False,
    file: str = "",
    gap: str = "4h",
    min_cluster: int = 3,
    min_age: str = "7d",
    dry_run: bool = True,
    ctx: Context = None,
) -> str:
    """KB maintenance: health checks, reindex, or compact.

    Args:
        action: "check" (default), "reindex", or "compact".
        checks: For action=check — "all", "quick" (T1+T2), "links", "sync", "quality".
        fix: For action=check — auto-fix safe issues.
        file: For action=compact — file to compact.
        gap: For compact — cluster gap (default "4h").
        min_cluster: For compact — min entries per cluster (default 3).
        min_age: For compact — min age for eligibility (default "7d").
        dry_run: For compact — preview only (default True).

    Returns:
        check: JSON with version, stats, checks[], fix_commands.
        reindex: JSON {status, message}.
        compact: JSON with cluster analysis or compaction results.
    """
    store = _get_store(ctx)
    try:
        if action == "reindex":
            result = store.reindex()
            return json.dumps({"status": "ok", "message": result})

        if action == "compact":
            if not file:
                return json.dumps({"status": "error", "message": "file is required for action=compact"})
            min_cluster = max(2, min(min_cluster, 50))
            sync_warning = ""
            if not dry_run and store.config.sync_enabled:
                sync_warning = "Sync is enabled. Other machines should sync before compaction."
            gap_seconds = _parse_duration_seconds(gap)
            min_age_seconds = _parse_duration_seconds(min_age)
            result = store.compact_file(
                file_name=file, gap_seconds=gap_seconds,
                min_cluster=min_cluster, min_age_seconds=min_age_seconds, dry_run=dry_run,
            )
            if sync_warning:
                result["sync_warning"] = sync_warning
            return json.dumps(result, indent=2)

        # Default: health check
        start = time.time()
        include_tier3 = checks in ("all", "quality")
        result = store.health_check(include_tier3=include_tier3, fix=fix)
        if checks in _HEALTH_CHECK_GROUPS:
            allowed = _HEALTH_CHECK_GROUPS[checks]
            result["checks"] = [c for c in result["checks"] if c["name"] in allowed]
            filtered = result["checks"]
            ok = sum(1 for c in filtered if c["status"] == "ok")
            warning = sum(1 for c in filtered if c["status"] == "warning")
            error = sum(1 for c in filtered if c["status"] == "error")
            total_issues = sum(c["count"] for c in filtered if c["status"] != "ok")
            result["summary"] = {
                "checks_run": len(filtered), "ok": ok, "warning": warning,
                "error": error, "total_issues": total_issues,
            }
        app_ctx = ctx.request_context.lifespan_context
        result["version"] = {
            "current": __version__,
            "update_available": getattr(app_ctx, "update_available", "") or "",
        }
        result["stats"] = _collect_system_stats(store, ctx)
        _enrich_fix_hints(result["checks"])
        result["duration_ms"] = round((time.time() - start) * 1000)
        return json.dumps(result, indent=2)
    except FileNotFoundError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except ValueError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp_server.tool()
def hkb_task(
    action: str = "list",
    title: str = "",
    description: str = "",
    file: str = "",
    epoch: int = 0,
    priority: str = "normal",
    blocked_by: str = "",
    status: str = "",
    note: str = "",
    domain: str = "",
    ctx: Context = None,
) -> str:
    """Task lifecycle: create, show, update, or list tasks.

    Args:
        action: "create", "show", "update", or "list" (default).
        title: Task title (action=create).
        description: Task description (action=create).
        file: Target file (create default: "tasks.default"; required for show/update).
        epoch: Entry epoch (required for show/update).
        priority: Priority (action=create) — low, normal, high, critical.
        blocked_by: Blocking task wiki-link (action=create).
        status: For update: new status. For list: filter (default "pending,in_progress,blocked").
        note: For update: note to append.
        domain: For list: namespace filter.

    Returns:
        create: JSON {status, file, epoch}.
        show: JSON with title, description, status, timeline.
        update: JSON {status, file, epoch}.
        list: JSON {_summary, tasks[]}.
    """
    store = _get_store(ctx)
    try:
        if action == "create":
            return _do_task_create(store, title, description, file or "tasks.default", priority, blocked_by)
        if action == "show":
            if not file or not epoch:
                return json.dumps({"status": "error", "message": "file and epoch required for show"})
            return _do_task_show(store, file, epoch)
        if action == "update":
            if not file or not epoch:
                return json.dumps({"status": "error", "message": "file and epoch required for update"})
            return _do_task_update(store, file, epoch, status, note)
        # Default: list
        return _do_task_list(store, file, status or "pending,in_progress,blocked", domain)
    except ValueError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


def _do_task_create(store, title, description, file, priority, blocked_by):
    if not store.db.file_exists(file):
        store.create_file(
            name=file,
            description=f"Task tracking file for {file.split('.', 1)[-1] if '.' in file else file}.",
            keywords=["task", "todo", "tracking"],
        )
    parts = ["@type: task", "@status: pending"]
    if priority != "normal":
        parts.append(f"@tags: priority-{priority}")
    parts.append(title)
    if description:
        parts.append(description)
    if blocked_by:
        parts.append(f"Depends: [[{blocked_by}]]")
    content = "\n".join(parts)
    result = store.add_entry(content=content, file_name=file)
    return json.dumps(parse_add_result(result))


def _do_task_show(store, file, epoch):
    db_entry = store.db.get_entry(file, epoch)
    if db_entry is None:
        return json.dumps({"status": "error", "message": f"Entry not found: {file} at epoch {epoch}"})
    if db_entry.get("entry_type") != "task":
        return json.dumps({"status": "error", "message": f"Entry at epoch {epoch} is not a task (type: {db_entry.get('entry_type', 'note')})."})
    import re
    content = db_entry.get("content", "")
    lines = content.split("\n")
    title = ""
    description_lines = []
    timeline = []
    dependencies = []
    timeline_re = re.compile(r"^\[(\d{4}-\d{2}-\d{2})\] Status → ([^:]+):\s*(.*)$")
    for line in lines:
        stripped = line.strip()
        m = timeline_re.match(stripped)
        if m:
            timeline.append({"date": m.group(1), "status": m.group(2).strip(), "note": m.group(3).strip()})
            continue
        if stripped.startswith("Depends:"):
            dep_links = re.findall(r"\[\[([^\]]+)\]\]", stripped)
            dependencies.extend(dep_links)
            continue
        if not title and stripped:
            title = stripped
        elif title:
            description_lines.append(line)
    description = "\n".join(description_lines).strip()
    return json.dumps({
        "file": file, "epoch": epoch, "title": title, "description": description,
        "status": db_entry.get("status", "pending"), "tags": db_entry.get("tags", ""),
        "dependencies": dependencies, "timeline": timeline,
    }, indent=2)


def _do_task_update(store, file, epoch, status, note):
    new_content = None
    if note:
        from datetime import date
        db_entry = store.db.get_entry(file, epoch)
        if db_entry is None:
            return json.dumps({"status": "error", "message": f"Entry not found: {file} at epoch {epoch}"})
        today = date.today().isoformat()
        status_label = status if status else db_entry.get("status", "unknown")
        new_content = f"{db_entry['content']}\n[{today}] Status → {status_label}: {note}"
    result = store.update_entry(file_name=file, epoch=epoch, new_content=new_content, set_status=status or None)
    return json.dumps(result)


def _do_task_list(store, file, status, domain):
    tasks = store.db.get_tasks(status_filter=status, file_name=file, domain=domain)
    output = [
        {"epoch": t["epoch"], "content": t["content"], "file_name": t["file_name"],
         "status": t.get("status", "pending"), "tags": t.get("tags", "")}
        for t in tasks
    ]
    summary = {"pending": 0, "in_progress": 0, "blocked": 0, "completed": 0, "cancelled": 0}
    for t in output:
        s = t["status"]
        if s in summary:
            summary[s] += 1
    return json.dumps({"_summary": summary, "tasks": output}, indent=2)


def _get_sync_engine(ctx: Context):
    """Get the SyncEngine from the sync worker, or create a one-shot engine."""
    app_ctx = ctx.request_context.lifespan_context
    worker = app_ctx.sync_worker
    if worker is not None:
        return worker.engine
    store = app_ctx.store
    config = store.config
    if not config.sync_enabled or not config.sync_bucket:
        return None
    try:
        from .sync import SyncEngine
        from .remote import S3Remote
        remote = S3Remote(
            bucket=config.sync_bucket, prefix=config.sync_prefix,
            region=config.sync_region, endpoint_url=config.sync_endpoint_url,
            access_key=config.sync_access_key, secret_key=config.sync_secret_key,
        )
        return SyncEngine(
            storage_dir=config.storage_dir, remote=remote,
            config=config, reindex_fn=store.reindex,
        )
    except Exception as e:
        logger.warning("Failed to create sync engine: %s", e)
        return None


@mcp_server.tool()
def hkb_sync(
    action: str = "both",
    dry_run: bool = False,
    key: str = "",
    value: str = "",
    conflict_action: str = "list",
    ctx: Context = None,
) -> str:
    """Sync operations: push/pull, status, config, conflicts.

    Args:
        action: "push", "pull", "both" (default), "status", "config", or "conflicts".
        dry_run: For push/pull — detect but don't apply.
        key: For action=config — config key to set.
        value: For action=config — value to set.
        conflict_action: For action=conflicts — "list" or "clear".

    Returns:
        push/pull/both: JSON with sync results.
        status: JSON with sync state.
        config: JSON with settings or confirmation.
        conflicts: JSON with conflict list or clear confirmation.
    """
    store = _get_store(ctx)
    try:
        if action == "status":
            return _do_sync_status(store, ctx)
        if action == "config":
            return _do_sync_config(store, key, value)
        if action == "conflicts":
            return _do_sync_conflicts(ctx, conflict_action)
        # push/pull/both
        if not store.config.sync_enabled:
            return json.dumps({"status": "error", "message": "Sync is not enabled. Use action=config to configure."})
        engine = _get_sync_engine(ctx)
        if engine is None:
            return json.dumps({"status": "error", "message": "Could not initialize sync engine."})
        if action not in ("push", "pull", "both"):
            return json.dumps({"status": "error", "message": "action must be push, pull, both, status, config, or conflicts."})
        result = engine.sync(direction=action, dry_run=dry_run)
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


def _do_sync_status(store, ctx):
    config = store.config
    result = {
        "sync_enabled": config.sync_enabled, "bucket": config.sync_bucket,
        "prefix": config.sync_prefix, "region": config.sync_region,
        "interval": config.sync_interval,
    }
    engine = _get_sync_engine(ctx)
    if engine is not None:
        try:
            status = engine.get_status()
            result.update(status)
        except Exception as e:
            result["engine_error"] = str(e)
    app_ctx = ctx.request_context.lifespan_context
    worker = app_ctx.sync_worker
    result["worker_running"] = worker is not None and worker.is_running if worker else False
    return json.dumps(result, indent=2)


def _do_sync_config(store, key, value):
    config = store.config
    if not key:
        from .crypto import mask_value, is_sensitive_field
        sync_fields = {
            "sync_enabled": config.sync_enabled, "sync_bucket": config.sync_bucket,
            "sync_prefix": config.sync_prefix, "sync_region": config.sync_region,
            "sync_endpoint_url": config.sync_endpoint_url,
            "sync_access_key": mask_value(config.sync_access_key) if config.sync_access_key else "(not set)",
            "sync_secret_key": mask_value(config.sync_secret_key) if config.sync_secret_key else "(not set)",
            "sync_interval": config.sync_interval, "sync_squash_threshold": config.sync_squash_threshold,
        }
        return json.dumps(sync_fields, indent=2)
    if not key.startswith("sync_"):
        return json.dumps({"status": "error", "message": f"Only sync_* keys can be set via this tool. Got: {key}"})
    if not hasattr(config, key):
        return json.dumps({"status": "error", "message": f"Unknown sync config key: {key}"})
    current = getattr(config, key)
    if isinstance(current, bool):
        value = value.lower() in ("true", "1", "yes")
    elif isinstance(current, int):
        value = int(value)
    elif isinstance(current, float):
        value = float(value)
    setattr(config, key, value)
    config.save()
    display_value = value
    from .crypto import is_sensitive_field
    if is_sensitive_field(key):
        display_value = "****" if value else "(not set)"
    return json.dumps({"status": "ok", "key": key, "value": display_value})


def _do_sync_conflicts(ctx, conflict_action):
    engine = _get_sync_engine(ctx)
    if engine is None:
        store = _get_store(ctx)
        if not store.config.sync_enabled:
            return json.dumps({"status": "error", "message": "Sync is not enabled."})
        return json.dumps({"conflicts": [], "count": 0})
    if conflict_action == "clear":
        count = engine.clear_conflict_log()
        return json.dumps({"status": "ok", "cleared": count})
    elif conflict_action == "list":
        conflicts = engine.get_conflict_log()
        return json.dumps({"conflicts": conflicts, "count": len(conflicts)}, indent=2)
    else:
        return json.dumps({"status": "error", "message": "conflict_action must be 'list' or 'clear'."})


@mcp_server.tool()
def hkb_context(
    topic: str,
    mode: str = "packed",
    max_tokens: int = 4000,
    domain: str = "",
    depth: str = "deep",
    top: int = 10,
    after: str = "",
    before: str = "",
    chronological: bool = True,
    ctx: Context = None,
) -> str:
    """Token-budgeted retrieval, file suggestions, or narrative reconstruction.

    Args:
        topic: Search query (required).
        mode: "packed" (default), "suggest", or "narrative".
        max_tokens: Token budget for packed mode (default 4000).
        domain: Namespace prefix filter.
        depth: packed: "deep"/"shallow"; narrative: 0-2 link hops (as string).
        top: Max results (suggest/narrative, default 10).
        after: Time filter start (narrative mode).
        before: Time filter end (narrative mode).
        chronological: Narrative sort by time (True) or relevance (False).

    Returns:
        packed: JSON with entries[], tokens_used.
        suggest: JSON with suggestions[].
        narrative: JSON with timeline[].
    """
    store = _get_store(ctx)
    try:
        if mode == "suggest":
            top = max(1, min(top, 20))
            result = store.suggest_context(task=topic, top=top)
            anchor_files = _get_anchor_files(ctx)
            _apply_anchor_boost(result.get("suggestions", []), anchor_files, key="file_name")
            return json.dumps(result, indent=2)

        if mode == "narrative":
            top = max(1, min(top, 500))
            try:
                depth_int = max(0, min(int(depth), 2))
            except (ValueError, TypeError):
                depth_int = 1
            after_epoch = _parse_time(after) or 0
            before_epoch = _parse_time(before) or 0
            result = store.build_narrative(
                topic=topic, chronological=chronological, depth=depth_int,
                limit=top, domain=domain,
                after_epoch=after_epoch, before_epoch=before_epoch,
            )
            return json.dumps(result, indent=2)

        # Default: packed
        max_tokens = max(100, min(max_tokens, 50000))
        if depth not in ("deep", "shallow"):
            depth = "deep"
        result = store.build_context(topic=topic, max_tokens=max_tokens, domain=domain, depth=depth)
        anchor_files = _get_anchor_files(ctx)
        _apply_anchor_boost(result.get("entries", []), anchor_files, key="file_name")
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


@mcp_server.tool()
def hkb_view(
    action: str = "list",
    name: str = "",
    files: list[str] = [],
    description: str = "",
    ctx: Context = None,
) -> str:
    """Named file groupings: set or list views.

    Args:
        action: "set" or "list" (default).
        name: View name (required for set; optional for list to show specific view).
        files: File names to include (action=set).
        description: View description (action=set).

    Returns:
        set: JSON {status, name, files, description}.
        list: JSON with views[] or single view details.
    """
    store = _get_store(ctx)
    try:
        if action == "set":
            if not name:
                return json.dumps({"status": "error", "message": "name is required for action=set"})
            result = store.set_view(name=name, files=list(files), description=description)
            return json.dumps(result, indent=2)
        # Default: list
        if name:
            view = store.get_view(name)
            if view:
                return json.dumps(view, indent=2)
            return json.dumps({"status": "error", "message": f"View '{name}' not found."})
        views = store.list_views()
        return json.dumps({"views": views, "count": len(views)}, indent=2)
    except ValueError as e:
        return json.dumps({"status": "error", "message": str(e)})
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None):
    """CLI entry point for the MCP server."""
    global _server_args
    _server_args = _parse_server_args(argv)

    # Handle SIGTERM/SIGINT gracefully — Claude Code sends SIGTERM on exit.
    # sys.exit() from a signal handler doesn't work reliably inside
    # asyncio's event loop, so we use os._exit() for an immediate clean
    # exit.  The DB connection is cleaned up by the OS; markdown files
    # (the source of truth) are already flushed on every write.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    signal.signal(signal.SIGINT, lambda *_: os._exit(0))

    try:
        mcp_server.run(transport="stdio")
    except KeyboardInterrupt:
        pass
    except SystemExit:
        pass
    except BrokenPipeError:
        # Client closed the pipe — normal during shutdown.
        pass


if __name__ == "__main__":
    main()
