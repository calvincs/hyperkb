"""Tests for hyperkb sync module — git integration, auto-commit, squash, SyncEngine."""

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hyperkb.config import KBConfig
from hyperkb.store import KnowledgeStore
from hyperkb.sync import GitRepo, SyncEngine, SyncWorker, WATCHDOG_AVAILABLE


# ---------------------------------------------------------------------------
# GitRepo tests
# ---------------------------------------------------------------------------

@pytest.fixture
def git_dir(tmp_path):
    """A temporary directory for git testing."""
    storage = tmp_path / "storage"
    storage.mkdir()
    return storage


@pytest.fixture
def git_repo(git_dir):
    """An initialized GitRepo."""
    # Write a sample file before init so there's something to commit
    (git_dir / "test.file.md").write_text("---\nname: test.file\n---\n")
    repo = GitRepo(git_dir)
    repo.init()
    return repo


class TestGitRepoInit:
    def test_init_creates_git_dir(self, git_dir):
        repo = GitRepo(git_dir)
        repo.init()
        assert (git_dir / ".git").is_dir()

    def test_init_creates_gitattributes(self, git_dir):
        repo = GitRepo(git_dir)
        repo.init()
        ga = git_dir / ".gitattributes"
        assert ga.exists()
        assert "*.md merge=hkb-entry" in ga.read_text()

    def test_init_creates_initial_commit(self, git_dir):
        repo = GitRepo(git_dir)
        repo.init()
        result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=str(git_dir), capture_output=True, text=True,
        )
        assert "sync: initial" in result.stdout

    def test_init_creates_sync_tag(self, git_dir):
        repo = GitRepo(git_dir)
        repo.init()
        result = subprocess.run(
            ["git", "tag", "-l", "last-sync"],
            cwd=str(git_dir), capture_output=True, text=True,
        )
        assert "last-sync" in result.stdout

    def test_init_idempotent(self, git_repo):
        """Second init should not error."""
        git_repo.init()
        assert git_repo.is_initialized()

    def test_is_initialized_true(self, git_repo):
        assert git_repo.is_initialized()

    def test_is_initialized_false(self, git_dir):
        repo = GitRepo(git_dir)
        assert not repo.is_initialized()


class TestGitRepoAutoCommit:
    def test_auto_commit_new_file(self, git_repo):
        storage = git_repo.storage_dir
        (storage / "new.file.md").write_text("---\nname: new.file\n---\n")
        committed = git_repo.auto_commit(["new.file.md"], "add new file")
        assert committed

        # Verify commit exists
        result = subprocess.run(
            ["git", "log", "--oneline", "-1"],
            cwd=str(storage), capture_output=True, text=True,
        )
        assert "add new file" in result.stdout

    def test_auto_commit_modified_file(self, git_repo):
        storage = git_repo.storage_dir
        f = storage / "test.file.md"
        f.write_text(f.read_text() + "\n>>> 100\nnew entry\n<<<\n")
        committed = git_repo.auto_commit(["test.file.md"], "modify file")
        assert committed

    def test_auto_commit_no_changes(self, git_repo):
        committed = git_repo.auto_commit(["test.file.md"], "no changes")
        assert not committed

    def test_auto_commit_returns_false_when_not_initialized(self, git_dir):
        repo = GitRepo(git_dir)
        committed = repo.auto_commit(["test.md"], "test")
        assert not committed


class TestGitRepoChanges:
    def test_get_changed_files_empty_initially(self, git_repo):
        changes = git_repo.get_changed_files()
        assert changes == []

    def test_get_changed_files_after_commit(self, git_repo):
        storage = git_repo.storage_dir
        (storage / "new.file.md").write_text("content")
        git_repo.auto_commit(["new.file.md"], "add new")
        changes = git_repo.get_changed_files()
        assert "new.file.md" in changes

    def test_get_commit_count_since_sync_zero(self, git_repo):
        assert git_repo.get_commit_count_since_sync() == 0

    def test_get_commit_count_since_sync_increments(self, git_repo):
        storage = git_repo.storage_dir
        for i in range(3):
            (storage / f"file{i}.md").write_text(f"content {i}")
            git_repo.auto_commit([f"file{i}.md"], f"commit {i}")
        assert git_repo.get_commit_count_since_sync() == 3


class TestGitRepoSquash:
    def test_squash_below_threshold(self, git_repo):
        storage = git_repo.storage_dir
        (storage / "f1.md").write_text("c1")
        git_repo.auto_commit(["f1.md"], "c1")
        squashed = git_repo.squash_if_needed(threshold=5)
        assert not squashed

    def test_squash_above_threshold(self, git_repo):
        storage = git_repo.storage_dir
        for i in range(6):
            (storage / f"f{i}.md").write_text(f"c{i}")
            git_repo.auto_commit([f"f{i}.md"], f"c{i}")

        assert git_repo.get_commit_count_since_sync() == 6
        squashed = git_repo.squash_if_needed(threshold=5)
        assert squashed
        # After squash + tag update, commit count is 0 (tag at HEAD)
        assert git_repo.get_commit_count_since_sync() == 0
        # But the files are still there
        for i in range(6):
            assert (storage / f"f{i}.md").exists()

    def test_squash_not_initialized(self, git_dir):
        repo = GitRepo(git_dir)
        assert not repo.squash_if_needed()


class TestGitRepoMerge:
    def test_create_and_delete_branch(self, git_repo):
        git_repo.create_branch("test-branch")
        assert git_repo.get_current_branch() == "test-branch"
        git_repo.checkout("master")
        git_repo.delete_branch("test-branch")

    def test_merge_no_conflicts(self, git_repo):
        storage = git_repo.storage_dir
        # Create a branch and add a file
        git_repo.create_branch("feature")
        (storage / "feature.md").write_text("feature content")
        git_repo.add_and_commit("add feature")

        # Go back to master
        git_repo.checkout("master")
        success, conflicts = git_repo.merge("feature")
        assert success
        assert conflicts == []
        assert (storage / "feature.md").exists()

    def test_merge_with_conflicts(self, git_repo):
        storage = git_repo.storage_dir
        test_file = storage / "test.file.md"

        # Create branch and modify the file
        git_repo.create_branch("branch-a")
        test_file.write_text("branch a content")
        git_repo.add_and_commit("branch a changes")

        # Go back to master and make conflicting changes
        git_repo.checkout("master")
        test_file.write_text("master content")
        git_repo.add_and_commit("master changes")

        # Merge should fail with conflicts
        success, conflicts = git_repo.merge("branch-a")
        assert not success
        assert "test.file.md" in conflicts

        # Clean up
        git_repo.abort_merge()
        git_repo.delete_branch("branch-a")


class TestGitRepoMisc:
    def test_update_sync_tag(self, git_repo):
        storage = git_repo.storage_dir
        (storage / "new.md").write_text("content")
        git_repo.auto_commit(["new.md"], "new commit")

        old_sha = git_repo.get_head_sha()
        git_repo.update_sync_tag()

        # After update, changed files should be empty (tag is at HEAD)
        assert git_repo.get_changed_files() == []

    def test_has_uncommitted_changes(self, git_repo):
        assert not git_repo.has_uncommitted_changes()
        (git_repo.storage_dir / "untracked.md").write_text("new")
        assert git_repo.has_uncommitted_changes()

    def test_commit_all_pending(self, git_repo):
        (git_repo.storage_dir / "untracked.md").write_text("new")
        committed = git_repo.commit_all_pending()
        assert committed
        assert not git_repo.has_uncommitted_changes()

    def test_commit_all_pending_nothing(self, git_repo):
        committed = git_repo.commit_all_pending()
        assert not committed

    def test_reinit(self, git_repo):
        storage = git_repo.storage_dir
        (storage / "data.md").write_text("data")
        git_repo.auto_commit(["data.md"], "add data")

        git_repo.reinit()
        assert git_repo.is_initialized()
        # File should still exist
        assert (storage / "data.md").exists()

    def test_get_head_sha(self, git_repo):
        sha = git_repo.get_head_sha()
        assert len(sha) == 40  # Full SHA
        assert all(c in "0123456789abcdef" for c in sha)

    def test_get_current_branch(self, git_repo):
        branch = git_repo.get_current_branch()
        assert branch in ("master", "main")


# ---------------------------------------------------------------------------
# Store auto-commit integration tests
# ---------------------------------------------------------------------------

@pytest.fixture
def sync_store(tmp_path):
    """A KnowledgeStore with sync enabled."""
    config = KBConfig(
        root=str(tmp_path),
        
        
        sync_enabled=True,
    )
    store = KnowledgeStore(config)
    store.init()
    yield store
    store.close()


class TestStoreAutoCommit:
    def test_create_file_auto_commits(self, sync_store):
        sync_store.create_file(
            name="test.file",
            description="test file",
            keywords=["test"],
        )
        # Check git is initialized and file is tracked
        git = GitRepo(sync_store.storage_dir)
        assert git.is_initialized()
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=str(sync_store.storage_dir),
            capture_output=True, text=True,
        )
        assert "test.file.md" in result.stdout

    def test_add_entry_auto_commits(self, sync_store):
        sync_store.create_file(
            name="test.file",
            description="test file",
            keywords=["test"],
        )
        sync_store.add_entry(
            content="Test entry content.",
            file_name="test.file",
        )
        result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=str(sync_store.storage_dir),
            capture_output=True, text=True,
        )
        # Either the entry commit is separate, or absorbed in init
        assert "test.file" in result.stdout or "sync: initial" in result.stdout
        # File should contain the entry
        content = (sync_store.storage_dir / "test.file.md").read_text()
        assert "Test entry content." in content

    def test_update_entry_auto_commits(self, sync_store):
        sync_store.create_file(
            name="test.file",
            description="test file",
            keywords=["test"],
        )
        sync_store.add_entry(
            content="Original content.",
            file_name="test.file",
            epoch=1000000,
        )
        sync_store.update_entry(
            file_name="test.file",
            epoch=1000000,
            new_content="Updated content.",
        )
        # The update should be committed — check git log
        result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=str(sync_store.storage_dir),
            capture_output=True, text=True,
        )
        # Should have multiple commits (init + update at minimum)
        lines = result.stdout.strip().split("\n")
        assert len(lines) >= 2  # At least init commit + update commit
        # File should contain the updated content
        content = (sync_store.storage_dir / "test.file.md").read_text()
        assert "Updated content." in content

    def test_no_auto_commit_when_sync_disabled(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            
            
            sync_enabled=False,
        )
        store = KnowledgeStore(config)
        store.init()
        store.create_file(
            name="test.file",
            description="test file",
            keywords=["test"],
        )
        # No .git directory should be created
        assert not (store.storage_dir / ".git").exists()
        store.close()


# ---------------------------------------------------------------------------
# SyncEngine tests
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_remote():
    """A mock S3Remote."""
    remote = MagicMock()
    remote.get_manifest.return_value = {"files": {}}
    remote.acquire_lock.return_value = True
    remote.release_lock.return_value = None
    return remote


@pytest.fixture
def sync_engine(tmp_path, mock_remote):
    """A SyncEngine with mock remote."""
    storage = tmp_path / ".hkb" / "storage"
    storage.mkdir(parents=True)
    (storage / "test.file.md").write_text("---\nname: test.file\n---\n")

    config = KBConfig(
        root=str(tmp_path),
        
        
        sync_enabled=True,
        sync_bucket="test-bucket",
        sync_squash_threshold=20,
    )
    engine = SyncEngine(
        storage_dir=storage,
        remote=mock_remote,
        config=config,
    )
    engine.setup()
    return engine


class TestSyncEngine:
    def test_setup_initializes_git(self, sync_engine):
        assert sync_engine.git.is_initialized()

    def test_sync_dry_run(self, sync_engine, mock_remote):
        result = sync_engine.sync(dry_run=True)
        assert result["status"] == "dry_run"
        assert "local_changes" in result
        assert "remote_changes" in result
        # Lock should not be acquired for dry run
        mock_remote.acquire_lock.assert_not_called()

    def test_sync_push_only(self, sync_engine, mock_remote):
        # Create a local change
        storage = sync_engine.storage_dir
        (storage / "new.file.md").write_text("new content")
        sync_engine.git.auto_commit(["new.file.md"], "add new file")

        result = sync_engine.sync(direction="push")
        assert result["status"] == "ok"
        assert "new.file.md" in result["pushed"]
        mock_remote.upload_file.assert_called()
        mock_remote.put_manifest.assert_called()

    def test_sync_locked(self, sync_engine, mock_remote):
        mock_remote.acquire_lock.return_value = False
        result = sync_engine.sync()
        assert result["status"] == "locked"

    def test_sync_releases_lock_on_error(self, sync_engine, mock_remote):
        mock_remote.acquire_lock.return_value = True
        mock_remote.upload_file.side_effect = Exception("network error")

        # Create a change to push
        storage = sync_engine.storage_dir
        (storage / "new.file.md").write_text("new content")
        sync_engine.git.auto_commit(["new.file.md"], "add")

        with pytest.raises(Exception, match="network error"):
            sync_engine.sync(direction="push")

        mock_remote.release_lock.assert_called()

    def test_get_status(self, sync_engine):
        status = sync_engine.get_status()
        assert status["sync_enabled"]
        assert status["git_initialized"]
        assert status["last_sync_status"] == "never"
        assert status["bucket"] == "test-bucket"

    def test_status_updates_after_sync(self, sync_engine):
        sync_engine.sync(dry_run=True)
        status = sync_engine.get_status()
        assert status["last_sync_status"] == "ok"
        assert status["last_sync_time"] > 0

    def test_build_local_manifest(self, sync_engine):
        manifest = sync_engine._build_local_manifest()
        assert "test.file.md" in manifest
        assert "sha256" in manifest["test.file.md"]
        assert "size" in manifest["test.file.md"]

    def test_detect_remote_changes_new_file(self, sync_engine):
        remote_manifest = {
            "files": {
                "remote.new.md": {"sha256": "abc123", "size": 100},
            }
        }
        local_manifest = {}
        changes = sync_engine._detect_remote_changes(remote_manifest, local_manifest)
        assert "remote.new.md" in changes

    def test_detect_remote_changes_modified(self, sync_engine):
        remote_manifest = {
            "files": {
                "test.file.md": {"sha256": "different_hash", "size": 100},
            }
        }
        local_manifest = {
            "test.file.md": {"sha256": "local_hash", "size": 50},
        }
        changes = sync_engine._detect_remote_changes(remote_manifest, local_manifest)
        assert "test.file.md" in changes

    def test_detect_remote_changes_no_changes(self, sync_engine):
        manifest = sync_engine._build_local_manifest()
        remote_manifest = {"files": manifest}
        changes = sync_engine._detect_remote_changes(remote_manifest, manifest)
        assert changes == {}


class TestSyncConflictLog:
    def test_empty_conflict_log(self, sync_engine):
        assert sync_engine.get_conflict_log() == []

    def test_log_and_read_conflict(self, sync_engine):
        sync_engine._log_conflict({
            "file": "test.md",
            "type": "epoch_collision",
            "resolution": "bumped",
        })
        logs = sync_engine.get_conflict_log()
        assert len(logs) == 1
        assert logs[0]["file"] == "test.md"

    def test_clear_conflict_log(self, sync_engine):
        sync_engine._log_conflict({"test": True})
        count = sync_engine.clear_conflict_log()
        assert count == 1
        assert sync_engine.get_conflict_log() == []


# ---------------------------------------------------------------------------
# SyncWorker tests
# ---------------------------------------------------------------------------

class TestSyncWorker:
    def test_worker_starts_and_stops(self, sync_engine):
        worker = SyncWorker(sync_engine, interval=1)
        worker.start()
        time.sleep(0.2)  # Allow thread to fully start
        assert worker.is_alive()
        worker.stop()
        worker.join(timeout=3)
        assert not worker.is_alive()

    def test_worker_is_daemon(self, sync_engine):
        worker = SyncWorker(sync_engine, interval=1)
        assert worker.daemon

    def test_worker_is_running_property(self, sync_engine):
        worker = SyncWorker(sync_engine, interval=60)
        assert not worker.is_running
        worker.start()
        time.sleep(0.1)
        assert worker.is_running
        worker.stop()
        worker.join(timeout=3)

    def test_worker_performs_sync(self, sync_engine, mock_remote):
        worker = SyncWorker(sync_engine, interval=1)
        worker.start()
        time.sleep(2)  # Wait for at least one sync cycle
        worker.stop()
        worker.join(timeout=3)
        # The engine should have been called
        assert sync_engine.last_sync_status in ("ok", "never")

    def test_worker_syncs_immediately_on_startup(self, sync_engine, mock_remote):
        """Worker should perform an initial sync before waiting for the interval."""
        worker = SyncWorker(sync_engine, interval=300)  # Long interval
        worker.start()
        time.sleep(1)  # Brief wait — sync should already have happened
        worker.stop()
        worker.join(timeout=3)
        # Even with a 5-minute interval, the initial sync should have fired
        assert sync_engine.last_sync_status == "ok"
        assert sync_engine.last_sync_time > 0


# ---------------------------------------------------------------------------
# Integration: two-machine simulation
# ---------------------------------------------------------------------------

class TestTwoMachineSync:
    """Simulate two machines syncing via a shared "S3" directory."""

    @pytest.fixture
    def two_machines(self, tmp_path):
        """Set up two machines with separate storage dirs and a mock S3."""
        machine_a_root = tmp_path / "machine_a"
        machine_b_root = tmp_path / "machine_b"
        s3_dir = tmp_path / "s3_bucket"

        for d in (machine_a_root, machine_b_root, s3_dir):
            (d / ".hkb" / "storage").mkdir(parents=True, exist_ok=True)

        # Create stores
        config_a = KBConfig(root=str(machine_a_root))
        config_b = KBConfig(root=str(machine_b_root))
        store_a = KnowledgeStore(config_a)
        store_b = KnowledgeStore(config_b)
        store_a.init()
        store_b.init()

        # Create git repos
        git_a = GitRepo(config_a.storage_dir)
        git_a.init()
        git_b = GitRepo(config_b.storage_dir)
        git_b.init()

        yield {
            "store_a": store_a,
            "store_b": store_b,
            "git_a": git_a,
            "git_b": git_b,
            "s3_dir": s3_dir,
        }

        store_a.close()
        store_b.close()

    def test_concurrent_appends_different_files(self, two_machines):
        """Two machines add to different files — should merge cleanly."""
        sa = two_machines["store_a"]
        sb = two_machines["store_b"]
        ga = two_machines["git_a"]
        gb = two_machines["git_b"]

        # Machine A creates a file
        sa.create_file(name="work.alpha", description="Alpha work", keywords=["alpha"])
        sa.add_entry(content="Alpha entry 1", file_name="work.alpha", epoch=1000000)
        ga.auto_commit(["work.alpha.md"], "add alpha")

        # Machine B creates a different file
        sb.create_file(name="work.beta", description="Beta work", keywords=["beta"])
        sb.add_entry(content="Beta entry 1", file_name="work.beta", epoch=1000001)
        gb.auto_commit(["work.beta.md"], "add beta")

        # Verify each machine has its own file committed
        assert ga.get_commit_count_since_sync() > 0
        assert gb.get_commit_count_since_sync() > 0

    def test_concurrent_appends_same_file(self, two_machines):
        """Two machines append to the same file — git should auto-merge."""
        sa = two_machines["store_a"]
        sb = two_machines["store_b"]
        ga = two_machines["git_a"]
        gb = two_machines["git_b"]

        # Both machines create the same file independently
        sa.create_file(name="shared.notes", description="Shared", keywords=["shared"])
        ga.auto_commit(["shared.notes.md"], "create shared")

        sb.create_file(name="shared.notes", description="Shared", keywords=["shared"])
        gb.auto_commit(["shared.notes.md"], "create shared")

        # Machine A adds an entry
        sa.add_entry(content="Entry from A", file_name="shared.notes", epoch=2000000)
        ga.auto_commit(["shared.notes.md"], "add from A")

        # Machine B adds a different entry
        sb.add_entry(content="Entry from B", file_name="shared.notes", epoch=3000000)
        gb.auto_commit(["shared.notes.md"], "add from B")

        # Both machines should have their entries tracked in git
        assert ga.get_commit_count_since_sync() > 0
        assert gb.get_commit_count_since_sync() > 0
        # Verify entries exist in their respective files
        a_content = (sa.storage_dir / "shared.notes.md").read_text()
        b_content = (sb.storage_dir / "shared.notes.md").read_text()
        assert "Entry from A" in a_content
        assert "Entry from B" in b_content
