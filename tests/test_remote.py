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
            "version": 2,
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
        remote.upgrade_protocol()

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

    def test_same_machine_cannot_reacquire(self, remote):
        assert remote.acquire_lock()
        assert not remote.acquire_lock()  # Every operation needs exclusive ownership
        assert remote.renew_lock()
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


class TestSdkContract:
    def test_s3_model_supports_conditional_lease_and_manifest_operations(self):
        """The declared boto3 floor must expose all fencing preconditions.

        Official boto3/botocore 1.40.0 wheel models were checked for this contract;
        botocore 1.34.0 lacks all three members and cannot provide safe sync.
        """
        import botocore.session
        model = botocore.session.get_session().get_service_model("s3")
        assert {"IfMatch", "IfNoneMatch"} <= set(model.operation_model("PutObject").input_shape.members)
        assert "IfMatch" in model.operation_model("DeleteObject").input_shape.members


def _metadata(remote, name, value):
    remote._client.put_object(Bucket=remote.bucket, Key=remote._sync_key(name),
                              Body=json.dumps(value).encode())


class TestProtocolFence:
    def test_empty_check_is_read_only(self, remote):
        with patch.object(remote._client, "put_object") as put, patch.object(remote._client, "delete_object") as delete:
            assert remote.check_protocol() == {"protocol_version": 2, "marker_present": False}
        put.assert_not_called()
        delete.assert_not_called()

    def test_ready_marker_uses_one_get_and_ignores_manifest_body(self, remote):
        _metadata(remote, "protocol.json", {"protocol_version": 2})
        _metadata(remote, "manifest.json", {"version": 2, "files": "large opaque body"})
        with patch.object(remote._protocol_client, "get_object", wraps=remote._protocol_client.get_object) as get:
            assert remote.check_protocol() == {"protocol_version": 2, "marker_present": True}
        assert get.call_count == 1
        assert get.call_args.kwargs["Key"].endswith("protocol.json")

    @pytest.mark.parametrize("version,status", [(1, "store_upgrade_required"), (3, "upgrade_required"),
                                               (True, "protocol_unverified"), (2.0, "protocol_unverified")])
    def test_marker_requires_exact_version(self, remote, version, status):
        from hyperkb.protocol import ProtocolError
        _metadata(remote, "protocol.json", {"protocol_version": version, "state": "unknown"})
        with pytest.raises(ProtocolError) as error:
            remote.check_protocol()
        assert error.value.status == status

    def test_legacy_manifest_requires_explicit_migration(self, remote):
        from hyperkb.protocol import ProtocolError
        _metadata(remote, "manifest.json", {"files": {}})
        with pytest.raises(ProtocolError) as error:
            remote.check_protocol()
        assert error.value.status == "store_upgrade_required"
        _metadata(remote, "manifest.json", {"version": 2, "files": {}})
        assert remote.check_protocol() == {"protocol_version": 2, "marker_present": False}

    @pytest.mark.parametrize("key", ["storage/lost.md", "_sync/objects/lost"])
    def test_missing_inventory_with_objects_fails_closed(self, remote, key):
        from hyperkb.protocol import ProtocolError
        remote._client.put_object(Bucket=remote.bucket, Key=remote._key(key), Body=b"source")
        with pytest.raises(ProtocolError) as error:
            remote.check_protocol()
        assert error.value.status == "protocol_unverified"

    def test_ensure_requires_lease_and_seeds_marker(self, remote):
        from hyperkb.protocol import ProtocolError
        with pytest.raises(ProtocolError):
            remote.ensure_protocol()
        assert remote.acquire_lock()
        try:
            assert remote.ensure_protocol() == {"protocol_version": 2, "marker_present": True}
        finally:
            remote.release_lock()

    def test_ensure_checks_manifest_header_even_with_ready_marker(self, remote):
        from hyperkb.protocol import ProtocolError
        _metadata(remote, "protocol.json", {"protocol_version": 2, "state": "ready"})
        _metadata(remote, "manifest.json", {"version": 3, "future_body": []})
        assert remote.acquire_lock()
        try:
            with pytest.raises(ProtocolError) as error:
                remote.ensure_protocol()
            assert error.value.status == "upgrade_required"
            with pytest.raises(ProtocolError) as error:
                remote.get_manifest()
            assert error.value.status == "upgrade_required"
        finally:
            remote.release_lock()

    def test_marker_create_race_rechecks_winning_version(self, remote):
        from hyperkb.protocol import ProtocolError
        write = remote._write_protocol
        def race(state, etag):
            _metadata(remote, "protocol.json", {"protocol_version": 3})
            return write(state, etag)
        assert remote.acquire_lock()
        try:
            with patch.object(remote, "_write_protocol", side_effect=race), pytest.raises(ProtocolError) as error:
                remote.ensure_protocol()
            assert error.value.status == "upgrade_required"
        finally:
            remote.release_lock()

    def test_transport_outage_distinct_from_auth_and_malformed_metadata(self, remote):
        from botocore.exceptions import EndpointConnectionError, ClientError
        from hyperkb.protocol import ProtocolError, ProtocolUnavailable
        with patch.object(remote._protocol_client, "get_object", side_effect=EndpointConnectionError(endpoint_url="https://example.invalid")):
            with pytest.raises(ProtocolUnavailable):
                remote.check_protocol()
        for status, code, expected in [(503, "ServiceUnavailable", ProtocolUnavailable), (403, "AccessDenied", ProtocolError)]:
            failure = ClientError({"Error": {"Code": code, "Message": "sensitive details"},
                                   "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject")
            with patch.object(remote._protocol_client, "get_object", side_effect=failure), pytest.raises(expected) as error:
                remote.check_protocol()
            assert type(error.value) is expected
            assert "sensitive details" not in str(error.value)
        remote._client.put_object(Bucket=remote.bucket, Key=remote._sync_key("protocol.json"), Body=b"invalid json")
        with pytest.raises(ProtocolError) as error:
            remote.check_protocol()
        assert not isinstance(error.value, ProtocolUnavailable)


class TestProtocolMigration:
    @staticmethod
    def legacy(remote):
        value = {"files": {"legacy.md": {"sha256": "a" * 64, "size": 12},
                           "blob.md": {"sha256": "b" * 64, "blob": "b" * 64}},
                 "tombstones": {"gone.md": {"sha256": "c" * 64, "blob": "c" * 64}},
                 "custom_inventory_metadata": {"preserve": True}}
        _metadata(remote, "manifest.json", value)
        return value

    def test_migration_preserves_inventory_and_is_idempotent(self, remote):
        old = self.legacy(remote)
        result = remote.upgrade_protocol()
        current = remote.get_manifest()
        assert current["version"] == 2
        for key, value in old.items():
            assert current[key] == value
        assert result["transformation"] == {"from_protocol_version": 1, "to_protocol_version": 2,
            "markdown_changed": False, "files_preserved": 2, "tombstones_preserved": 1,
            "blob_references_preserved": 2}
        assert remote.upgrade_protocol()["upgraded"] is False

    def test_dry_run_does_not_acquire_lease_or_write(self, remote):
        self.legacy(remote)
        with patch.object(remote, "acquire_lock") as acquire, \
             patch.object(remote._client, "put_object") as put, \
             patch.object(remote._client, "delete_object") as delete:
            plan = remote.upgrade_protocol(dry_run=True)
        assert plan["status"] == "dry_run"
        assert plan["transformation"]["files_preserved"] == 2
        acquire.assert_not_called()
        put.assert_not_called()
        delete.assert_not_called()

    @pytest.mark.parametrize("fail_phase", ["manifest", "ready"])
    def test_interrupted_migration_stays_fenced_and_retry_finishes(self, remote, fail_phase):
        from hyperkb.protocol import ProtocolError
        old = self.legacy(remote)
        write = remote._write_protocol
        def fail_ready(state, etag):
            if state == "ready":
                raise OSError("interrupted")
            return write(state, etag)
        target = "put_manifest" if fail_phase == "manifest" else "_write_protocol"
        failure = OSError("interrupted") if fail_phase == "manifest" else fail_ready
        with patch.object(remote, target, side_effect=failure), pytest.raises(ProtocolError):
            remote.upgrade_protocol()
        with pytest.raises(ProtocolError) as error:
            remote.check_protocol()
        assert error.value.status == "protocol_unverified"
        assert "incomplete" in str(error.value)
        remote.upgrade_protocol()
        assert remote.check_protocol()["protocol_version"] == 2
        current = remote.get_manifest()
        for key, value in old.items():
            assert current[key] == value

    def test_future_marker_aborts_before_reading_future_manifest(self, remote):
        from hyperkb.protocol import ProtocolError
        _metadata(remote, "protocol.json", {"protocol_version": 3, "state": "new-state"})
        remote._client.put_object(Bucket=remote.bucket, Key=remote._sync_key("manifest.json"), Body=b"unknown future encoding")
        with patch.object(remote._client, "put_object") as put, \
             patch.object(remote._client, "delete_object") as delete, \
             pytest.raises(ProtocolError) as error:
            remote.upgrade_protocol()
        assert error.value.status == "upgrade_required"
        put.assert_not_called()
        delete.assert_not_called()
        assert remote._read_json("protocol.json")[0]["protocol_version"] == 3

    def test_manifest_cas_prevents_lost_inventory_during_migration(self, remote):
        from hyperkb.protocol import ProtocolError
        self.legacy(remote)
        put = remote.put_manifest
        newer = {"version": 1, "files": {"new.md": {"sha256": "d" * 64}}}
        def race(value, **kwargs):
            _metadata(remote, "manifest.json", newer)
            return put(value, **kwargs)
        with patch.object(remote, "put_manifest", side_effect=race), pytest.raises(ProtocolError):
            remote.upgrade_protocol()
        assert remote._read_json("manifest.json")[0] == newer
        remote.upgrade_protocol()
        assert remote.get_manifest()["files"] == newer["files"]


def test_protocol_probe_uses_short_timeouts_without_bulk_retries(remote):
    probe = remote._protocol_client.meta.config
    bulk = remote._client.meta.config
    assert (probe.connect_timeout, probe.read_timeout) == (2, 2)
    assert probe.retries["total_max_attempts"] == 1
    assert (bulk.connect_timeout, bulk.read_timeout) == (5, 15)
    assert bulk.retries["total_max_attempts"] == 2
    assert remote._protocol_client.meta.endpoint_url == remote._client.meta.endpoint_url
    assert remote._protocol_client.meta.region_name == remote._client.meta.region_name
