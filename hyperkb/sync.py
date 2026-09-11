"""Multi-machine sync with local Git history and immutable S3 snapshots.

A shared storage lock serializes store mutations, snapshot publication, Git and
reindexing across local processes. One background worker holds the leader lock;
remote operations additionally require a unique, renewable conditional S3 lease.
Remote baselines are persisted separately from Git tags so pull-only operations
never acknowledge pending uploads. Downloads are staged and hash checked before
complete-file three-way merging and atomic publication into live storage.
"""

import json
import base64
import hashlib
import tempfile
from contextlib import contextmanager
import logging
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .locking import storage_lock, FileLock

logger = logging.getLogger(__name__)

# Graceful import of optional deps
WATCHDOG_AVAILABLE = False
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    pass


class GitRepo:
    """Manages a git repository in the storage directory for change tracking.

    Provides auto-commit after writes, squash to prevent bloat, tag management,
    and change detection via git diff.
    """

    SYNC_TAG = "last-sync"

    def __init__(self, storage_dir: Path):
        self.storage_dir = storage_dir
        self._initialized = False

    def _run(
        self,
        args: list[str],
        check: bool = True,
        capture: bool = True,
    ) -> subprocess.CompletedProcess:
        """Run a git command in the storage directory."""
        cmd = ["git"] + args
        return subprocess.run(
            cmd,
            cwd=str(self.storage_dir),
            capture_output=capture,
            text=True,
            check=check,
            timeout=30,
        )

    def is_initialized(self) -> bool:
        """Check if storage dir is already a git repo."""
        git_dir = self.storage_dir / ".git"
        return git_dir.exists()

    def init(self) -> None:
        """Initialize git repo in storage dir if not already done."""
        if self.is_initialized():
            self._ignore_coordination_files()
            self._initialized = True
            return

        self._run(["init"])
        self._ignore_coordination_files()

        # Configure git user for commits (local only, doesn't affect global)
        self._run(["config", "user.email", "hyperkb@local"])
        self._run(["config", "user.name", "hyperkb"])

        # Create .gitattributes for entry-aware merge
        gitattributes = self.storage_dir / ".gitattributes"
        gitattributes.write_text("*.md merge=hkb-entry\n")

        # Initial commit
        self._run(["add", "-A"])
        result = self._run(["status", "--porcelain"], check=False)
        if result.stdout.strip():
            self._run(["commit", "-m", "sync: initial"])
        else:
            # Nothing to commit — create an empty initial commit
            self._run(["commit", "--allow-empty", "-m", "sync: initial"])

        # Tag initial sync point
        self._run(["tag", self.SYNC_TAG])
        self._initialized = True

    def _ignore_coordination_files(self):
        exclude = self.storage_dir / ".git" / "info" / "exclude"
        exclude.parent.mkdir(parents=True, exist_ok=True)
        existing = exclude.read_text() if exclude.exists() else ""
        additions = [pattern for pattern in (".hkb-storage.lock", ".sync-*")
                     if pattern not in existing.splitlines()]
        if additions:
            with exclude.open("a") as stream:
                stream.write("\n" + "\n".join(additions) + "\n")

    def auto_commit(self, files: list[str], message: str) -> bool:
        """Commit specific files after a store write operation.

        Args:
            files: List of filenames (relative to storage_dir) to commit.
            message: Commit message.

        Returns:
            True if a commit was created, False if nothing to commit.
        """
        if not self.is_initialized():
            return False

        # Stage the specific files
        for f in files:
            filepath = self.storage_dir / f
            if filepath.exists():
                self._run(["add", f])
            else:
                # File was deleted — stage the removal
                self._run(["add", f], check=False)

        # Check if there's anything staged
        result = self._run(["diff", "--cached", "--name-only"], check=False)
        if not result.stdout.strip():
            return False

        self._run(["commit", "-m", message])
        return True

    def get_changed_files(self) -> list[str]:
        """Get files changed since last sync point.

        Returns list of filenames relative to storage_dir.
        """
        if not self.is_initialized():
            return []

        # Check if the sync tag exists
        result = self._run(["tag", "-l", self.SYNC_TAG], check=False)
        if not result.stdout.strip():
            # No sync tag — all files are "changed"
            result = self._run(["ls-files"], check=False)
            return [f for f in result.stdout.strip().splitlines() if f]

        result = self._run(
            ["diff", "--name-only", self.SYNC_TAG, "HEAD"],
            check=False,
        )
        return [f for f in result.stdout.strip().splitlines() if f]

    def get_commit_count_since_sync(self) -> int:
        """Count commits between last-sync tag and HEAD."""
        if not self.is_initialized():
            return 0

        result = self._run(
            ["rev-list", "--count", f"{self.SYNC_TAG}..HEAD"],
            check=False,
        )
        try:
            return int(result.stdout.strip())
        except (ValueError, AttributeError):
            return 0

    def squash_if_needed(self, threshold: int = 20) -> bool:
        """Squash history to prevent .git bloat.

        Keeps git history lean: soft reset to last-sync, recommit everything
        as one commit. Only squashes when commit count exceeds threshold.

        Returns True if squash was performed.
        """
        if not self.is_initialized():
            return False

        count = self.get_commit_count_since_sync()
        if count <= threshold:
            return False

        self._run(["reset", "--soft", self.SYNC_TAG])
        self._run(["commit", "-m", "sync: squashed local changes"])
        self._run(["tag", "-f", self.SYNC_TAG])
        self._run(["gc", "--auto"], check=False)
        return True

    def update_sync_tag(self) -> None:
        """Move the last-sync tag to HEAD."""
        if not self.is_initialized():
            return
        self._run(["tag", "-f", self.SYNC_TAG])

    def create_branch(self, name: str, start: str = "") -> None:
        """Create and checkout a new branch."""
        args = ["checkout", "-b", name]
        if start:
            args.append(start)
        self._run(args)

    def checkout(self, ref: str) -> None:
        """Checkout a branch or ref."""
        self._run(["checkout", ref])

    def delete_branch(self, name: str) -> None:
        """Delete a local branch."""
        self._run(["branch", "-D", name], check=False)

    def merge(self, branch: str) -> tuple[bool, list[str]]:
        """Merge a branch into current branch.

        Returns (success, conflicted_files).
        If success is True, merge completed (auto or no conflicts).
        If success is False, conflicted_files lists files needing resolution.
        """
        result = self._run(["merge", branch, "--no-edit"], check=False)
        if result.returncode == 0:
            return True, []

        # Merge conflict — get list of conflicted files
        status = self._run(["diff", "--name-only", "--diff-filter=U"], check=False)
        conflicted = [f for f in status.stdout.strip().splitlines() if f]
        return False, conflicted

    def add_and_commit(self, message: str) -> bool:
        """Stage all changes and commit."""
        self._run(["add", "-A"])
        result = self._run(["diff", "--cached", "--name-only"], check=False)
        if not result.stdout.strip():
            return False
        self._run(["commit", "-m", message])
        return True

    def abort_merge(self) -> None:
        """Abort an in-progress merge."""
        self._run(["merge", "--abort"], check=False)

    def get_current_branch(self) -> str:
        """Get the name of the current branch."""
        result = self._run(["rev-parse", "--abbrev-ref", "HEAD"], check=False)
        return result.stdout.strip()

    def get_head_sha(self) -> str:
        """Get the SHA of HEAD."""
        result = self._run(["rev-parse", "HEAD"], check=False)
        return result.stdout.strip()

    def has_uncommitted_changes(self) -> bool:
        """Check for uncommitted changes in the working tree."""
        result = self._run(["status", "--porcelain"], check=False)
        return bool(result.stdout.strip())

    def commit_all_pending(self, message: str = "sync: pending changes") -> bool:
        """Stage and commit any uncommitted changes."""
        if not self.has_uncommitted_changes():
            return False
        return self.add_and_commit(message)

    def reinit(self) -> None:
        """Re-initialize git repo from scratch (recovery from corrupt .git).

        Removes .git, re-inits, commits current state, tags as last-sync.
        """
        git_dir = self.storage_dir / ".git"
        if git_dir.exists():
            shutil.rmtree(git_dir)
        self._initialized = False
        self.init()


class SyncEngine:
    """Synchronize a locked storage snapshot against a versioned remote manifest.

    Acquire the remote lease before reading inventory, stage remote downloads,
    merge against the last observed per-target baseline, publish atomically and
    reindex, then upload pending changes and acknowledge the resulting inventory.
    """

    def __init__(
        self,
        storage_dir: Path,
        remote,  # S3Remote instance
        config,  # KBConfig
        reindex_fn=None,  # Callback to reindex store after sync
    ):
        self.storage_dir = storage_dir
        self.remote = remote
        self.config = config
        self.reindex_fn = reindex_fn
        self.git = GitRepo(storage_dir)
        self._lock = threading.Lock()
        self._last_sync_time: float = 0
        self._last_sync_status: str = "never"
        self._last_sync_error: str = ""
        self._conflict_log_dir = storage_dir.parent / "sync" / "conflicts"
        self._remote_settings = self._settings()
        self._cancel_event = threading.Event()

    @property
    def last_sync_time(self) -> float:
        return self._last_sync_time

    @property
    def last_sync_status(self) -> str:
        return self._last_sync_status

    @property
    def last_sync_error(self) -> str:
        return self._last_sync_error

    def _settings(self):
        return tuple(getattr(self.config, "sync_" + key) for key in (
            "bucket", "prefix", "region", "endpoint_url", "access_key", "secret_key"))

    def _refresh_config(self):
        if self.config.config_path.exists():
            loaded = type(self.config).load(self.config.root)
            for key, value in vars(loaded).items():
                if key.startswith("sync_"):
                    setattr(self.config, key, value)
        settings = self._settings()
        if settings != self._remote_settings and self.config.sync_enabled:
            from .remote import S3Remote
            self.remote = S3Remote(*settings)
            self._remote_settings = settings

    def _guard(self):
        """Check cancellation/configuration and fence each remote operation."""
        if self._cancel_event.is_set():
            raise InterruptedError("Sync worker stopped")
        if self.config.config_path.exists():
            latest = type(self.config).load(self.config.root)
            if not latest.sync_enabled or any(
                getattr(latest, "sync_" + key) != value
                for key, value in zip(("bucket", "prefix", "region", "endpoint_url",
                                       "access_key", "secret_key"), self._remote_settings)
            ):
                raise InterruptedError("Sync configuration changed; retry with current settings")
        if not self.config.sync_enabled:
            raise InterruptedError("Sync disabled")
        if not self.remote.renew_lock():
            raise RuntimeError("Remote sync lease lost")

    @contextmanager
    def _mutation(self):
        lock = storage_lock(self.storage_dir)
        deadline = time.monotonic() + 60
        while not lock.acquire(timeout=0.1):
            if self._cancel_event.is_set():
                raise InterruptedError("Sync worker stopped")
            if time.monotonic() >= deadline:
                raise TimeoutError("Storage is busy")
        try:
            yield
        finally:
            lock.release()

    def setup(self) -> None:
        with storage_lock(self.storage_dir):
            self.git.init()

    def sync(self, direction: str = "both", dry_run: bool = False) -> dict:
        if direction not in ("push", "pull", "both"):
            raise ValueError("direction must be push, pull, or both")
        with self._lock, self._mutation():
            try:
                self._refresh_config()
                if not self.config.sync_enabled or self._cancel_event.is_set():
                    return {"status": "disabled", "direction": direction}
                self.git.init()
                self.git.commit_all_pending()
                dirty = self.storage_dir.parent / "sync" / "reindex-needed"
                if dirty.exists() and self.reindex_fn:
                    self.reindex_fn()
                    dirty.unlink()
                result = self._do_sync(direction, dry_run)
                self._last_sync_time = time.time()
                self._last_sync_status = "ok" if dry_run else result["status"]
                self._last_sync_error = ""
                return result
            except Exception as e:
                self._last_sync_status = "error"
                self._last_sync_error = str(e)
                logger.error("Sync failed: %s", e)
                raise

    @staticmethod
    def _validate_name(name):
        if (not isinstance(name, str) or not name.endswith(".md") or
                name.startswith(".") or "/" in name or "\\" in name or
                "\x00" in name or Path(name).name != name):
            raise ValueError(f"Unsafe remote filename: {name!r}")

    @staticmethod
    def _sha(content):
        return hashlib.sha256(content).hexdigest() if content is not None else None

    def _state_path(self):
        # Baselines belong to a destination, never to credentials or a machine.
        target = (self.config.sync_bucket, self.config.sync_prefix.rstrip("/"),
                  self.config.sync_region, self.config.sync_endpoint_url)
        key = hashlib.sha256(json.dumps(target).encode()).hexdigest()
        return self.storage_dir.parent / "sync" / ("baseline-" + key + ".json")

    def _load_baseline(self):
        path = self._state_path()
        if not path.exists():
            # A legacy last-sync tag does not prove an upload completed. Treat
            # migration as first sync, preserving local and remote additions.
            return {}
        data = json.loads(path.read_text())
        result = {}
        for name, content in data.items():
            self._validate_name(name)
            result[name] = base64.b64decode(content, validate=True)
        return result

    @staticmethod
    def _atomic_write(path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(prefix=".sync-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, path)
            if os.name != "nt":
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)

    def _save_baseline(self, baseline):
        self._atomic_write(self._state_path(), json.dumps({
            name: base64.b64encode(content).decode("ascii")
            for name, content in baseline.items()
        }).encode())

    def _do_sync(self, direction: str, dry_run: bool) -> dict:
        if not dry_run and not self.remote.acquire_lock():
            return {"status": "locked", "message": "Another sync holds the remote lease."}
        try:
            if not dry_run:
                self._guard()
            # Always read inventory after acquiring ownership.
            remote_manifest = self.remote.get_manifest()
            from .remote import S3Remote
            manifest_etag = self.remote.manifest_etag if isinstance(self.remote, S3Remote) else None
            if (not isinstance(remote_manifest, dict) or type(remote_manifest.get("version", 1)) is not int
                    or remote_manifest.get("version", 1) not in (1, 2)):
                raise ValueError("Unsupported remote manifest version")
            remote_files = remote_manifest.get("files", {})
            tombstones = remote_manifest.get("tombstones", {})
            if not isinstance(tombstones, dict):
                raise ValueError("Invalid remote tombstones")
            for name, entry in tombstones.items():
                self._validate_name(name)
                if not isinstance(entry, dict):
                    raise ValueError("Invalid remote tombstone")
            if not isinstance(remote_files, dict):
                raise ValueError("Invalid remote manifest")
            for name, entry in remote_files.items():
                self._validate_name(name)
                if not isinstance(entry, dict) or not isinstance(entry.get("sha256"), str):
                    raise ValueError(f"Invalid manifest entry: {name}")
            baseline = self._load_baseline()
            local = {}
            for path in self.storage_dir.glob("*.md"):
                self._validate_name(path.name)
                if path.is_symlink():
                    raise ValueError(f"Refusing symlink in storage: {path.name}")
                local[path.name] = path.read_bytes()
            for name, entry in tombstones.items():
                if name not in baseline and name in local and self._sha(local[name]) == entry.get("sha256"):
                    baseline[name] = local[name]
            names = sorted(set(local) | set(baseline) | set(remote_files))
            local_changes = [n for n in names if local.get(n) != baseline.get(n)]
            remote_changes = [n for n in names if
                remote_files.get(n, {}).get("sha256") != self._sha(baseline.get(n))]
            if dry_run:
                return {"status": "dry_run", "local_changes": local_changes,
                        "remote_changes": remote_changes, "direction": direction}

            # Download and validate everything before touching live files.
            incoming = {}
            with tempfile.TemporaryDirectory(prefix="hkb-sync-") as staging:
                for name in remote_changes:
                    if name not in remote_files:
                        incoming[name] = None
                        continue
                    self._guard()
                    content = (self.remote.download_version(entry_blob)
                               if (entry_blob := remote_files[name].get("blob"))
                               else self.remote.download_file(name))
                    entry = remote_files[name]
                    if content is None or self._sha(content) != entry["sha256"]:
                        raise ValueError(f"Download hash mismatch or missing object: {name}")
                    if "size" in entry and len(content) != entry["size"]:
                        raise ValueError(f"Download size mismatch: {name}")
                    (Path(staging) / name).write_bytes(content)
                    incoming[name] = content

                pulled, deleted, pushed, conflicts = [], [], [], []
                next_baseline = dict(baseline)
                desired = dict(local)
                if direction in ("pull", "both"):
                    from .conflict import merge_versions
                    for name in remote_changes:
                        before, ours, theirs = baseline.get(name), local.get(name), incoming[name]
                        merged, info = merge_versions(before, ours, theirs, name)
                        if info:
                            conflicts.append(info)
                            self._log_conflict(info)
                        if merged is None:
                            desired.pop(name, None)
                        else:
                            desired[name] = merged
                        if theirs is None:
                            next_baseline.pop(name, None)
                        else:
                            next_baseline[name] = theirs
                    self._guard()
                    dirty = self.storage_dir.parent / "sync" / "reindex-needed"
                    if local != desired:
                        self._atomic_write(dirty, b"reindex needed\n")
                    for name in sorted(set(local) | set(desired)):
                        if local.get(name) == desired.get(name):
                            continue
                        if name in desired:
                            self._atomic_write(self.storage_dir / name, desired[name])
                            pulled.append(name)
                        else:
                            (self.storage_dir / name).unlink()
                            deleted.append(name)
                    if pulled or deleted:
                        self.git.commit_all_pending("sync: apply remote changes")
                        # Index every disk change, including deletion-only pulls.
                        if self.reindex_fn:
                            self.reindex_fn()
                            dirty.unlink()
                    self._save_baseline(next_baseline)

                if direction in ("push", "both"):
                    changes = [n for n in sorted(set(desired) | set(next_baseline))
                               if desired.get(n) != next_baseline.get(n)]
                    # Push alone must not clobber concurrent changes unseen locally.
                    blocked = [n for n in changes if direction == "push" and
                               n in remote_changes and self._sha(desired.get(n)) !=
                               remote_files.get(n, {}).get("sha256")]
                    if blocked:
                        raise RuntimeError("Concurrent remote changes; run sync both: " + ", ".join(blocked))
                    manifest = dict(remote_manifest)
                    inventory = dict(remote_files)
                    deleted_inventory = dict(tombstones)
                    if changes and not remote_manifest.get("version") and not remote_files:
                        self._guard()
                        # Establish an explicit empty inventory before creating
                        # immutable blobs, so a crash on first upload is retryable.
                        manifest_etag = self._publish_manifest(
                            {"version": 2, "files": {}, "tombstones": tombstones}, manifest_etag)
                    for name in changes:
                        self._guard()
                        content = desired.get(name)
                        if content is None:
                            # Publish deletion through inventory first. Keeping the
                            # unreferenced object makes interrupted publication safe.
                            removed = inventory.pop(name, None)
                            if removed:
                                deleted_inventory[name] = dict(removed, deleted_at=datetime.now(timezone.utc).isoformat())
                            next_baseline.pop(name, None)
                        else:
                            from .remote import S3Remote
                            inventory[name] = {"sha256": self._sha(content), "size": len(content)}
                            if isinstance(self.remote, S3Remote):
                                inventory[name]["blob"] = self.remote.upload_version(content)
                            else:
                                self.remote.upload_file(name, content)
                            next_baseline[name] = content
                            deleted_inventory.pop(name, None)
                        pushed.append(name)
                    if changes:
                        self._guard()
                        manifest.update(version=2, files=inventory, tombstones=deleted_inventory,
                                        last_sync=datetime.now(timezone.utc).isoformat(),
                                        machine_id=self._get_machine_id())
                        self._publish_manifest(manifest, manifest_etag)
                        for name in changes:
                            if name not in desired:
                                self._guard()
                                self.remote.delete_file(name)
                self._save_baseline(next_baseline)
                # A Git tag is only a convenience checkpoint once every local
                # file matches the acknowledged remote state.
                if desired == next_baseline:
                    self.git.update_sync_tag()
                return {"status": "ok", "pushed": pushed, "pulled": pulled,
                        "deleted": deleted, "conflicts": conflicts, "direction": direction}
        finally:
            if not dry_run:
                self.remote.release_lock()

    def _publish_manifest(self, manifest, expected_etag):
        from .remote import S3Remote
        if isinstance(self.remote, S3Remote):
            return self.remote.put_manifest(manifest, expected_etag=expected_etag)
        return self.remote.put_manifest(manifest)

    def _build_local_manifest(self) -> dict:
        return {path.name: {"sha256": self._sha(path.read_bytes()),
                            "size": path.stat().st_size, "modified": path.stat().st_mtime}
                for path in sorted(self.storage_dir.glob("*.md"))}

    def _detect_remote_changes(self, remote_manifest: dict, local_manifest: dict) -> dict:
        # Compatibility helper: absence only represents deletion after a prior
        # baseline explicitly recorded that file.
        changes = {name: entry for name, entry in remote_manifest.get("files", {}).items()
                   if entry.get("sha256") != local_manifest.get(name, {}).get("sha256")}
        for name in self._load_baseline():
            if name not in remote_manifest.get("files", {}):
                changes[name] = {"deleted": True}
        return changes

    def _log_conflict(self, conflict_info: dict) -> None:
        """Log conflict details to ~/.hkb/sync/conflicts/ for review."""
        self._conflict_log_dir.mkdir(parents=True, exist_ok=True)
        ts = time.time_ns()
        log_file = self._conflict_log_dir / f"conflict_{ts}.json"
        log_file.write_text(json.dumps(conflict_info, indent=2))

    def get_conflict_log(self) -> list[dict]:
        """Read all logged conflicts."""
        if not self._conflict_log_dir.exists():
            return []
        conflicts = []
        for f in sorted(self._conflict_log_dir.glob("*.json")):
            try:
                conflicts.append(json.loads(f.read_text()))
            except (json.JSONDecodeError, OSError):
                continue
        return conflicts

    def clear_conflict_log(self) -> int:
        """Clear all conflict logs. Returns count of files removed."""
        if not self._conflict_log_dir.exists():
            return 0
        count = 0
        for f in self._conflict_log_dir.glob("*.json"):
            f.unlink()
            count += 1
        return count

    def get_status(self) -> dict:
        """Get current sync status."""
        with storage_lock(self.storage_dir):
            baseline = self._load_baseline()
            local = self._build_local_manifest()
            local_changes = sorted(name for name in set(baseline) | set(local)
                                   if self._sha(baseline.get(name)) != local.get(name, {}).get("sha256"))
        commit_count = self.git.get_commit_count_since_sync() if self.git.is_initialized() else 0

        return {
            "sync_enabled": self.config.sync_enabled,
            "git_initialized": self.git.is_initialized(),
            "last_sync_time": self._last_sync_time,
            "last_sync_status": self._last_sync_status,
            "last_sync_error": self._last_sync_error,
            "local_pending_files": local_changes,
            "local_pending_count": len(local_changes),
            "commits_since_sync": commit_count,
            "bucket": self.config.sync_bucket,
            "prefix": self.config.sync_prefix,
        }

    @staticmethod
    def _get_machine_id() -> str:
        """Get a unique machine identifier."""
        from .crypto import _get_machine_key_material
        return _get_machine_key_material()


class SyncWorker(threading.Thread):
    """Background thread that periodically syncs with S3.

    Two trigger mechanisms:
    1. Filesystem watcher (watchdog): detects local changes, debounces, triggers sync
    2. Interval polling: checks remote manifest every N seconds

    Degrades gracefully: no watchdog = interval only, no network = accumulate locally.
    """

    def __init__(self, engine: SyncEngine, interval: int = 60):
        super().__init__(daemon=True, name="hkb-sync-worker")
        self.engine = engine
        self.interval = interval
        self._stop_event = threading.Event()
        self._sync_requested = threading.Event()
        self._observer = None
        self._debounce_timer: Optional[threading.Timer] = None
        self._debounce_seconds = 5.0
        self._is_leader = False

    def run(self):
        """Elect one process as leader; waiting clients take over after release."""
        leader = FileLock(self.engine.storage_dir.parent / "sync" / "leader.lock",
                          timeout=0, name="sync leader")
        try:
            while not self._stop_event.is_set():
                if not self._is_leader:
                    if not leader.try_acquire():
                        self._sync_requested.wait(timeout=min(self.interval, 1.0))
                        self._sync_requested.clear()
                        continue
                    self._is_leader = True
                    if WATCHDOG_AVAILABLE:
                        self._start_watcher()
                self._sync_requested.clear()
                try:
                    self.engine.sync()
                except Exception as e:
                    logger.error("Background sync failed: %s", e)
                if self._stop_event.is_set():
                    break
                # A distinct wake event never clears the sticky shutdown flag.
                interval = min(self.interval, self.engine.config.sync_interval)
                self._sync_requested.wait(timeout=max(0.05, interval))
        finally:
            was_leader = self._is_leader
            self._is_leader = False
            if self._observer:
                self._observer.stop()
                self._observer.join(timeout=2)
                self._observer = None
            if was_leader:
                leader.release()

    def stop(self):
        """Request cancellation without waiting for network or active mutations."""
        self._stop_event.set()
        self.engine._cancel_event.set()
        self._sync_requested.set()
        if self._debounce_timer:
            self._debounce_timer.cancel()

    def request_sync(self):
        self._sync_requested.set()

    @property
    def is_leader(self):
        return self._is_leader

    def _start_watcher(self):
        """Start filesystem watcher on storage directory."""
        if not WATCHDOG_AVAILABLE:
            return

        handler = _StorageChangeHandler(self)
        self._observer = Observer()
        self._observer.schedule(
            handler, str(self.engine.storage_dir), recursive=False
        )
        self._observer.start()
        logger.info("Filesystem watcher started on %s", self.engine.storage_dir)

    def _on_fs_change(self):
        """Called by filesystem handler (debounced)."""
        if self._stop_event.is_set():
            return
        if self._debounce_timer:
            self._debounce_timer.cancel()
        self._debounce_timer = threading.Timer(
            self._debounce_seconds, self.request_sync
        )
        self._debounce_timer.daemon = True
        self._debounce_timer.start()

    @property
    def is_running(self) -> bool:
        return self.is_alive() and not self._stop_event.is_set()


if WATCHDOG_AVAILABLE:
    class _StorageChangeHandler(FileSystemEventHandler):
        """Watches for .md file changes in the storage directory."""

        def __init__(self, worker: SyncWorker):
            self.worker = worker

        def on_modified(self, event):
            if not event.is_directory and event.src_path.endswith(".md"):
                self.worker._on_fs_change()

        def on_created(self, event):
            if not event.is_directory and event.src_path.endswith(".md"):
                self.worker._on_fs_change()

        def on_deleted(self, event):
            if not event.is_directory and event.src_path.endswith(".md"):
                self.worker._on_fs_change()

        def on_moved(self, event):
            if not event.is_directory and (event.src_path.endswith(".md") or event.dest_path.endswith(".md")):
                self.worker._on_fs_change()
