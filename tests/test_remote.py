"""Tests for hyperkb remote module — S3 operations with moto mocking."""

import hashlib
import json
import time
from unittest.mock import patch, MagicMock

import pytest

# Skip all tests if boto3/moto not available
boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws
from hyperkb.remote import S3Remote, LOCK_TTL_SECONDS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BUCKET = "test-hkb-bucket"
PREFIX = "hkb/"
REGION = "us-east-1"


@pytest.fixture
def s3_env():
    """Set up mocked S3 environment."""
    with mock_aws():
        # Create the bucket
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def remote(s3_env):
    """An S3Remote connected to the mocked bucket."""
    return S3Remote(
        bucket=BUCKET,
        prefix=PREFIX,
        region=REGION,
    )


# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------

class TestFileOperations:
    def test_upload_and_download(self, remote):
        content = b"---\nname: test.file\n---\n\n>>> 100\nhello world\n<<<\n"
        remote.upload_file("test.file.md", content)

        downloaded = remote.download_file("test.file.md")
        assert downloaded == content

    def test_download_nonexistent(self, remote):
        result = remote.download_file("nonexistent.md")
        assert result is None

    def test_delete_file(self, remote):
        remote.upload_file("to-delete.md", b"content")
        remote.delete_file("to-delete.md")
        assert remote.download_file("to-delete.md") is None

    def test_list_files_empty(self, remote):
        files = remote.list_files()
        assert files == []

    def test_list_files(self, remote):
        remote.upload_file("alpha.md", b"a")
        remote.upload_file("beta.md", b"b")
        remote.upload_file("gamma.md", b"c")

        files = remote.list_files()
        assert sorted(files) == ["alpha.md", "beta.md", "gamma.md"]

    def test_list_files_ignores_non_md(self, remote, s3_env):
        remote.upload_file("file.md", b"content")
        # Upload a non-md file directly
        s3_env.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}storage/file.txt",
            Body=b"not markdown",
        )
        files = remote.list_files()
        assert files == ["file.md"]

    def test_upload_large_file(self, remote):
        content = b"x" * 100_000
        remote.upload_file("large.md", content)
        downloaded = remote.download_file("large.md")
        assert downloaded == content
        assert len(downloaded) == 100_000


# ---------------------------------------------------------------------------
# Manifest operations
# ---------------------------------------------------------------------------

class TestManifest:
    def test_get_manifest_empty(self, remote):
        manifest = remote.get_manifest()
        assert manifest == {"files": {}}

    def test_put_and_get_manifest(self, remote):
        manifest = {
            "files": {
                "test.md": {"sha256": "abc123", "size": 42},
            },
            "last_sync": "2026-02-25T12:00:00Z",
        }
        remote.put_manifest(manifest)

        loaded = remote.get_manifest()
        assert loaded["files"]["test.md"]["sha256"] == "abc123"
        assert loaded["last_sync"] == "2026-02-25T12:00:00Z"

    def test_rebuild_manifest(self, remote):
        content_a = b"content of file a"
        content_b = b"content of file b"
        remote.upload_file("a.md", content_a)
        remote.upload_file("b.md", content_b)

        manifest = remote.rebuild_manifest()
        assert "a.md" in manifest["files"]
        assert "b.md" in manifest["files"]
        assert manifest["files"]["a.md"]["sha256"] == hashlib.sha256(content_a).hexdigest()
        assert manifest["files"]["b.md"]["sha256"] == hashlib.sha256(content_b).hexdigest()
        assert manifest["rebuilt"] is True

    def test_rebuild_manifest_stores_to_s3(self, remote):
        remote.upload_file("x.md", b"data")
        remote.rebuild_manifest()

        # Should be readable via get_manifest
        manifest = remote.get_manifest()
        assert "x.md" in manifest["files"]


# ---------------------------------------------------------------------------
# Advisory locking
# ---------------------------------------------------------------------------

class TestLocking:
    def test_acquire_and_release(self, remote):
        assert remote.acquire_lock()
        remote.release_lock()

    def test_same_machine_can_reacquire(self, remote):
        assert remote.acquire_lock()
        assert remote.acquire_lock()  # Should succeed (same machine)
        remote.release_lock()

    def test_different_machine_blocked(self, remote, s3_env):
        # Simulate another machine's lock
        lock_data = {
            "machine_id": "other-machine-12345",
            "timestamp": time.time(),
            "ttl": LOCK_TTL_SECONDS,
        }
        s3_env.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}_sync/lock.json",
            Body=json.dumps(lock_data).encode(),
        )

        # Our machine should be blocked
        assert not remote.acquire_lock()

    def test_stale_lock_broken(self, remote, s3_env):
        # Simulate a stale lock (old timestamp)
        lock_data = {
            "machine_id": "other-machine-12345",
            "timestamp": time.time() - LOCK_TTL_SECONDS - 60,
            "ttl": LOCK_TTL_SECONDS,
        }
        s3_env.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}_sync/lock.json",
            Body=json.dumps(lock_data).encode(),
        )

        # Should break the stale lock and acquire
        assert remote.acquire_lock()

    def test_check_lock_none_when_unlocked(self, remote):
        assert remote.check_lock() is None

    def test_check_lock_returns_info(self, remote):
        remote.acquire_lock()
        info = remote.check_lock()
        assert info is not None
        assert "machine_id" in info
        assert "timestamp" in info

    def test_check_lock_none_for_stale(self, remote, s3_env):
        lock_data = {
            "machine_id": "old-machine",
            "timestamp": time.time() - LOCK_TTL_SECONDS - 60,
            "ttl": LOCK_TTL_SECONDS,
        }
        s3_env.put_object(
            Bucket=BUCKET,
            Key=f"{PREFIX}_sync/lock.json",
            Body=json.dumps(lock_data).encode(),
        )
        assert remote.check_lock() is None


# ---------------------------------------------------------------------------
# Utility methods
# ---------------------------------------------------------------------------

class TestUtilities:
    def test_verify_download_correct(self, remote):
        content = b"test content"
        sha = hashlib.sha256(content).hexdigest()
        assert remote.verify_download(content, sha)

    def test_verify_download_incorrect(self, remote):
        content = b"test content"
        assert not remote.verify_download(content, "wrong_hash")

    def test_check_connectivity(self, remote):
        assert remote.check_connectivity()

    def test_key_building(self, remote):
        assert remote._storage_key("test.md") == "hkb/storage/test.md"
        assert remote._sync_key("manifest.json") == "hkb/_sync/manifest.json"

    def test_custom_prefix(self, s3_env):
        remote = S3Remote(bucket=BUCKET, prefix="custom/prefix/", region=REGION)
        assert remote._storage_key("test.md") == "custom/prefix/storage/test.md"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_no_boto3_raises(self):
        with patch("hyperkb.remote.BOTO3_AVAILABLE", False):
            with pytest.raises(RuntimeError, match="boto3 not installed"):
                S3Remote(bucket="test", prefix="hkb/")

    def test_release_lock_no_error_when_no_lock(self, remote):
        # Should not raise
        remote.release_lock()
