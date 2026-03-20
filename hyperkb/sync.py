"""Multi-machine sync for hyperkb via git locally + S3 remotely.

Git provides change tracking, diffing, and three-way merge.
S3 provides simple remote storage. Periodic squashing prevents .git bloat.

Architecture:
    ~/.hkb/storage/  (local git repo)
    S3 bucket/       (remote storage mirror + sync metadata)

Sync flow:
    1. LOCK    - Acquire S3 advisory lock
    2. DETECT  - Local changes via git diff, remote via manifest
    3. PULL    - Download remote → temp branch → git merge
    4. PUSH    - Upload merged state to S3
    5. FINALIZE - Tag sync point, squash old history, release lock
    6. REINDEX  - Rebuild SQLite from merged files
"""

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
            self._initialized = True
            return

        self._run(["init"])

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
    """Orchestrates push/pull/merge operations between local git and S3 remote.

    Sync flow:
        1. Acquire S3 lock
        2. Detect local + remote changes
        3. Pull remote changes → temp branch → git merge
        4. Push merged state to S3
        5. Update sync tag, squash history, release lock
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

    @property
    def last_sync_time(self) -> float:
        return self._last_sync_time

    @property
    def last_sync_status(self) -> str:
        return self._last_sync_status

    @property
    def last_sync_error(self) -> str:
        return self._last_sync_error

    def setup(self) -> None:
        """One-time setup: initialize git repo in storage dir."""
        self.git.init()

    def sync(self, direction: str = "both", dry_run: bool = False) -> dict:
        """Perform a sync operation.

        Args:
            direction: "push", "pull", or "both" (default).
            dry_run: If True, detect changes but don't apply.

        Returns:
            Dict with sync results.
        """
        with self._lock:
            try:
                self.git.init()  # Ensure git is ready
                self.git.commit_all_pending()  # Commit any uncommitted changes

                result = self._do_sync(direction, dry_run)
                self._last_sync_time = time.time()
                self._last_sync_status = "ok"
                self._last_sync_error = ""
                return result
            except Exception as e:
                self._last_sync_status = "error"
                self._last_sync_error = str(e)
                logger.error("Sync failed: %s", e)
                raise

    def _do_sync(self, direction: str, dry_run: bool) -> dict:
        """Internal sync implementation."""
        # 1. Detect changes
        local_changes = self.git.get_changed_files()
        local_changes = [f for f in local_changes if f.endswith(".md")]

        remote_manifest = self.remote.get_manifest()
        local_manifest = self._build_local_manifest()
        remote_changes = self._detect_remote_changes(remote_manifest, local_manifest)

        # First sync: if remote is empty, push all local files
        if not remote_manifest.get("files") and local_manifest:
            local_changes = list(local_manifest.keys())

        if dry_run:
            return {
                "status": "dry_run",
                "local_changes": local_changes,
                "remote_changes": list(remote_changes.keys()),
                "direction": direction,
            }

        # 2. Acquire lock
        lock_acquired = self.remote.acquire_lock()
        if not lock_acquired:
            return {
                "status": "locked",
                "message": "Another machine is currently syncing. Try again shortly.",
            }

        try:
            pushed = []
            pulled = []
            conflicts = []

            # 3. Pull (if direction allows)
            if direction in ("pull", "both") and remote_changes:
                pull_result = self._pull(remote_changes)
                pulled = pull_result.get("pulled", [])
                conflicts = pull_result.get("conflicts", [])

            # 4. Push (if direction allows)
            if direction in ("push", "both") and local_changes:
                push_result = self._push(local_changes)
                pushed = push_result.get("pushed", [])

            # 5. Finalize
            self.git.update_sync_tag()
            self.git.squash_if_needed(self.config.sync_squash_threshold)

            # 6. Reindex if we pulled changes
            if pulled and self.reindex_fn:
                try:
                    self.reindex_fn()
                except Exception as e:
                    logger.warning("Post-sync reindex failed: %s", e)

            return {
                "status": "ok",
                "pushed": pushed,
                "pulled": pulled,
                "conflicts": conflicts,
                "direction": direction,
            }
        finally:
            self.remote.release_lock()

    def _build_local_manifest(self) -> dict:
        """Build a manifest of local files with SHA256 hashes."""
        import hashlib
        manifest = {}
        for filepath in sorted(self.storage_dir.glob("*.md")):
            content = filepath.read_bytes()
            sha = hashlib.sha256(content).hexdigest()
            manifest[filepath.name] = {
                "sha256": sha,
                "size": len(content),
                "modified": filepath.stat().st_mtime,
            }
        return manifest

    def _detect_remote_changes(
        self, remote_manifest: dict, local_manifest: dict
    ) -> dict:
        """Compare remote manifest against local to find files changed remotely.

        Returns dict of {filename: remote_entry} for files that differ.
        """
        changes = {}
        remote_files = remote_manifest.get("files", {})
        for name, remote_entry in remote_files.items():
            local_entry = local_manifest.get(name)
            if local_entry is None:
                # New file on remote
                changes[name] = remote_entry
            elif local_entry["sha256"] != remote_entry.get("sha256"):
                # Modified on remote
                changes[name] = remote_entry
        # Detect remote deletions
        for name in local_manifest:
            if name not in remote_files and remote_manifest.get("files"):
                changes[name] = {"deleted": True}
        return changes

    def _pull(self, remote_changes: dict) -> dict:
        """Pull remote changes: download, create temp branch, merge."""
        from .conflict import resolve_conflicts

        pulled = []
        conflicts = []

        # Download changed files to a temp directory
        temp_dir = self.storage_dir.parent / "sync" / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            for filename, entry in remote_changes.items():
                if entry.get("deleted"):
                    # Remote deletion — remove locally
                    local_path = self.storage_dir / filename
                    if local_path.exists():
                        local_path.unlink()
                    pulled.append(filename)
                    continue

                # Download from S3
                content = self.remote.download_file(filename)
                if content is not None:
                    (temp_dir / filename).write_bytes(content)

            # Create a temp branch from last-sync, apply remote changes
            main_branch = self.git.get_current_branch()
            self.git.create_branch("remote-sync", self.git.SYNC_TAG)

            try:
                # Copy downloaded files into working tree
                for filepath in temp_dir.glob("*.md"):
                    shutil.copy2(str(filepath), str(self.storage_dir / filepath.name))

                # Remove files deleted remotely
                for filename, entry in remote_changes.items():
                    if entry.get("deleted"):
                        local_path = self.storage_dir / filename
                        if local_path.exists():
                            local_path.unlink()

                self.git.add_and_commit("sync: remote changes")

                # Switch back to main and merge
                self.git.checkout(main_branch)
                success, conflicted = self.git.merge("remote-sync")

                if not success and conflicted:
                    # Resolve conflicts using entry-aware resolver
                    for conflict_file in conflicted:
                        filepath = self.storage_dir / conflict_file
                        if filepath.exists():
                            resolved, conflict_info = resolve_conflicts(filepath)
                            if resolved:
                                filepath.write_text(resolved, encoding="utf-8")
                                conflicts.append(conflict_info)
                                self._log_conflict(conflict_info)

                    self.git.add_and_commit("sync: merged with conflict resolution")

                pulled = [
                    f for f in remote_changes.keys()
                    if not remote_changes[f].get("deleted")
                ]
            except Exception:
                # Cleanup on failure
                self.git.abort_merge()
                self.git.checkout(main_branch)
                raise
            finally:
                self.git.delete_branch("remote-sync")
        finally:
            # Clean up temp dir
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)

        return {"pulled": pulled, "conflicts": conflicts}

    def _push(self, local_changes: list[str]) -> dict:
        """Push local changes to S3."""
        pushed = []
        for filename in local_changes:
            filepath = self.storage_dir / filename
            if filepath.exists():
                content = filepath.read_bytes()
                # Verify hash before upload
                import hashlib
                sha = hashlib.sha256(content).hexdigest()
                self.remote.upload_file(filename, content)
                pushed.append(filename)
            else:
                # File was deleted locally — remove from S3
                self.remote.delete_file(filename)
                pushed.append(filename)

        # Update manifest
        manifest = self._build_local_manifest()
        self.remote.put_manifest({
            "files": manifest,
            "last_sync": datetime.now(timezone.utc).isoformat(),
            "machine_id": self._get_machine_id(),
        })

        return {"pushed": pushed}

    def _log_conflict(self, conflict_info: dict) -> None:
        """Log conflict details to ~/.hkb/sync/conflicts/ for review."""
        self._conflict_log_dir.mkdir(parents=True, exist_ok=True)
        ts = int(time.time())
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
        local_changes = self.git.get_changed_files() if self.git.is_initialized() else []
        local_changes = [f for f in local_changes if f.endswith(".md")]
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

    def run(self):
        """Main worker loop."""
        # Start filesystem watcher if available
        if WATCHDOG_AVAILABLE:
            self._start_watcher()

        # Perform an immediate sync on startup
        try:
            self.engine.sync()
        except Exception as e:
            logger.error("Initial background sync failed: %s", e)

        while not self._stop_event.is_set():
            # Wait for either: interval timeout, explicit sync request, or stop
            self._stop_event.wait(timeout=self.interval)

            if self._stop_event.is_set():
                break

            # Perform sync
            try:
                self.engine.sync()
            except Exception as e:
                logger.error("Background sync failed: %s", e)

            self._sync_requested.clear()

    def stop(self):
        """Stop the worker thread."""
        self._stop_event.set()
        if self._debounce_timer:
            self._debounce_timer.cancel()
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)

    def request_sync(self):
        """Request an immediate sync (debounced)."""
        self._sync_requested.set()
        self._stop_event.set()  # Wake up the wait
        self._stop_event.clear()  # Reset for next iteration

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
        if self._debounce_timer:
            self._debounce_timer.cancel()
        self._debounce_timer = threading.Timer(
            self._debounce_seconds, self.request_sync
        )
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
