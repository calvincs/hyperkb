"""S3-compatible sync storage with immutable snapshots and conditional leases.

Version 2 layout below the configured prefix:
    _sync/protocol.json       - exact client protocol and ready/upgrading fence
    _sync/manifest.json       - versioned live inventory and deletion tombstones
    _sync/objects/<sha256>    - immutable markdown bytes referenced by the manifest
    _sync/lock.json           - unique operation lease, TTL and diagnostic machine ID
    storage/*.md             - legacy version 1 objects (still readable)

Old clients must be upgraded before publishing version 2. Immutable content is
retained; rebuilding never guesses deleted paths or replaces a version 2 inventory
with a legacy object listing.
"""

import hashlib
import json
import logging
import time
import uuid
import threading
from typing import Optional
from contextlib import contextmanager

from .protocol import SYNC_PROTOCOL_VERSION, ProtocolError, require_matching_protocol

logger = logging.getLogger(__name__)

BOTO3_AVAILABLE = False
try:
    import boto3
    from botocore.exceptions import (ClientError, NoCredentialsError, EndpointConnectionError,
                                     ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError)
    from botocore.config import Config
    BOTO3_AVAILABLE = True
except ImportError:
    pass

LOCK_TTL_SECONDS = 300  # 5 minutes
_UNCONDITIONAL = object()


class S3Remote:
    """S3-compatible remote storage for sync.

    Handles file upload/download, manifest management, and advisory locking.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "hkb/",
        region: str = "",
        endpoint_url: str = "",
        access_key: str = "",
        secret_key: str = "",
    ):
        if not BOTO3_AVAILABLE:
            raise RuntimeError(
                "boto3 not installed. Install with: pip install 'hyperkb[sync]'"
            )

        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        self.region = region
        self.endpoint_url = endpoint_url
        self._machine_id = self._get_machine_id()
        self._lease_id = None
        self.manifest_etag = None
        self._lease_mutex = threading.RLock()

        # Build boto3 client kwargs
        kwargs = {"config": Config(connect_timeout=5, read_timeout=15,
                                    retries={"total_max_attempts": 2})}
        if region:
            kwargs["region_name"] = region
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key

        self._client = boto3.client("s3", **kwargs)
        # Knowledge operations probe compatibility before proceeding locally;
        # they must not inherit bulk-transfer retry and read-timeout delays.
        probe_kwargs = dict(kwargs)
        probe_kwargs["config"] = Config(connect_timeout=2, read_timeout=2,
                                        retries={"total_max_attempts": 1})
        self._protocol_client = boto3.client("s3", **probe_kwargs)

    def _key(self, path: str) -> str:
        """Build full S3 key from relative path."""
        return f"{self.prefix}{path}"

    def _sync_key(self, name: str) -> str:
        """Build key for sync metadata files."""
        return f"{self.prefix}_sync/{name}"

    def _storage_key(self, filename: str) -> str:
        """Build key for storage files."""
        return f"{self.prefix}storage/{filename}"

    # --- File operations ---

    def upload_file(self, filename: str, content: bytes) -> None:
        """Upload a file to S3 storage."""
        key = self._storage_key(filename)
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType="text/markdown",
        )
        logger.debug("Uploaded %s (%d bytes)", key, len(content))

    def download_file(self, filename: str) -> Optional[bytes]:
        """Download a file from S3 storage.

        Returns file content as bytes, or None if not found.
        """
        key = self._storage_key(filename)
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read()
            logger.debug("Downloaded %s (%d bytes)", key, len(content))
            return content
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def delete_file(self, filename: str) -> None:
        """Delete a file from S3 storage."""
        key = self._storage_key(filename)
        self._client.delete_object(Bucket=self.bucket, Key=key)
        logger.debug("Deleted %s", key)

    def list_files(self) -> list[str]:
        """List all .md files in S3 storage."""
        prefix = f"{self.prefix}storage/"
        files = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key[len(prefix):]
                if name.endswith(".md"):
                    files.append(name)
        return files

    # --- Stable protocol envelope ---

    @contextmanager
    def _protocol_errors(self):
        """Distinguish transport outages from untrusted or inaccessible metadata."""
        from .protocol import ProtocolUnavailable
        try:
            yield
        except ProtocolError:
            raise
        except (EndpointConnectionError, ConnectionClosedError, ConnectTimeoutError, ReadTimeoutError) as error:
            raise ProtocolUnavailable() from error
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if 500 <= status < 600:
                raise ProtocolUnavailable() from error
            raise ProtocolError("protocol_unverified", "S3 protocol verification failed. Check bucket permissions and configuration.") from error
        except Exception as error:
            raise ProtocolError("protocol_unverified", "S3 protocol metadata could not be verified. Check configuration or restore valid metadata.") from error

    def _read_json(self, name, client=None):
        client = self._client if client is None else client
        try:
            response = client.get_object(Bucket=self.bucket, Key=self._sync_key(name))
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None, None
            raise
        value = json.loads(response["Body"].read().decode("utf-8"))
        if not isinstance(value, dict):
            raise ProtocolError("protocol_unverified", "S3 protocol metadata must be a JSON object.")
        return value, response["ETag"]

    def _require_empty_remote(self, client=None):
        client = self._client if client is None else client
        for prefix in (self._key("storage/"), self._sync_key("objects/")):
            response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
            if response.get("KeyCount", 0):
                raise ProtocolError("protocol_unverified", "The S3 manifest is missing while stored objects exist. Restore the manifest or explicitly rebuild legacy inventory before syncing.")

    @staticmethod
    def _ready_marker(marker):
        version = require_matching_protocol(marker.get("protocol_version"))
        if marker.get("state", "ready") != "ready":
            raise ProtocolError("protocol_unverified", "The S3 protocol migration is incomplete. Local recording remains available; retry hkb sync upgrade-protocol before syncing.", version)
        return version

    def check_protocol(self) -> dict:
        """Read only: check the small stable marker, or a legacy manifest header.

        Ready markers are a one-GET path for knowledge requests. Sync separately
        validates the manifest header under its lease before using its inventory.
        """
        with self._protocol_errors():
            marker, _ = self._read_json("protocol.json", self._protocol_client)
            if marker is not None:
                return {"protocol_version": self._ready_marker(marker), "marker_present": True}
            manifest, _ = self._read_json("manifest.json", self._protocol_client)
            if manifest is not None:
                version = require_matching_protocol(manifest.get("version", 1))
            else:
                self._require_empty_remote(self._protocol_client)
                version = SYNC_PROTOCOL_VERSION
            return {"protocol_version": version, "marker_present": False}

    def _write_protocol(self, state, expected_etag):
        conditions = {"IfMatch": expected_etag} if expected_etag else {"IfNoneMatch": "*"}
        response = self._client.put_object(
            Bucket=self.bucket, Key=self._sync_key("protocol.json"),
            Body=json.dumps({"protocol_version": SYNC_PROTOCOL_VERSION, "state": state,
                             "revision": uuid.uuid4().hex}).encode(),
            ContentType="application/json", **conditions)
        return response["ETag"]

    def _require_lease(self):
        if not self.renew_lock():
            raise ProtocolError("protocol_unverified", "The S3 sync lease is unavailable or expired; retry the operation.")

    def ensure_protocol(self) -> dict:
        """Under our lease, validate inventory and conditionally seed a ready marker."""
        with self._protocol_errors():
            self._require_lease()
            result = self.check_protocol()
            manifest, _ = self._read_json("manifest.json")
            if manifest is not None:
                require_matching_protocol(manifest.get("version", 1))
            else:
                self._require_empty_remote()
            if not result["marker_present"]:
                self._require_lease()
                try:
                    self._write_protocol("ready", None)
                except ClientError as error:
                    if not self._condition_failed(error):
                        raise
                    # A concurrent creator wins; its envelope must match too.
                result = self.check_protocol()
                manifest, _ = self._read_json("manifest.json")
                if manifest is not None:
                    require_matching_protocol(manifest.get("version", 1))
                else:
                    self._require_empty_remote()
            return result

    def upgrade_protocol(self, dry_run: bool = False) -> dict:
        """Explicit, resumable v1 -> v2 migration; no object inventory is discarded.

        Fence guarded old and new clients with an upgrading marker, CAS the
        manifest, then CAS the marker to ready. A retry can finish either stage.
        """
        with self._protocol_errors():
            if not dry_run:
                # Reject future or malformed protocols before even writing a
                # lease. Re-read the complete snapshot after ownership below.
                self.upgrade_protocol(dry_run=True)
            if not dry_run and not self.acquire_lock():
                raise ProtocolError("protocol_unverified", "Another operation holds the S3 sync lease. Retry the protocol upgrade shortly.")
            try:
                marker, marker_etag = self._read_json("protocol.json")
                marker_version = marker.get("protocol_version") if marker is not None else None
                if marker is not None and (type(marker_version) is not int or marker_version not in (1, SYNC_PROTOCOL_VERSION)):
                    require_matching_protocol(marker_version)
                # A future marker is rejected before even reading its manifest.
                manifest, manifest_etag = self._read_json("manifest.json")
                manifest_version = manifest.get("version", 1) if manifest is not None else None
                if manifest is not None and (type(manifest_version) is not int or manifest_version not in (1, SYNC_PROTOCOL_VERSION)):
                    require_matching_protocol(manifest_version)
                if marker is not None and marker.get("state", "ready") not in ("ready", "upgrading"):
                    raise ProtocolError("protocol_unverified", "The S3 protocol marker has an unknown migration state.")
                if manifest is None:
                    self._require_empty_remote()
                    manifest = {"files": {}, "tombstones": {}}
                if not isinstance(manifest.get("files"), dict) or not isinstance(manifest.get("tombstones", {}), dict):
                    raise ProtocolError("protocol_unverified", "The S3 inventory is malformed; restore it before migrating.")
                transformation = {
                    "from_protocol_version": manifest_version,
                    "to_protocol_version": SYNC_PROTOCOL_VERSION,
                    "markdown_changed": False,
                    "files_preserved": len(manifest["files"]),
                    "tombstones_preserved": len(manifest.get("tombstones", {})),
                    "blob_references_preserved": sum(
                        bool(entry.get("blob")) for inventory in (manifest["files"], manifest.get("tombstones", {}))
                        for entry in inventory.values() if isinstance(entry, dict)),
                }
                if dry_run:
                    return {"status": "dry_run", "protocol_version": SYNC_PROTOCOL_VERSION,
                            "upgraded": False, "transformation": transformation}
                if marker_version == SYNC_PROTOCOL_VERSION and marker.get("state", "ready") == "ready" and manifest_version == SYNC_PROTOCOL_VERSION:
                    return {"status": "ok", "protocol_version": SYNC_PROTOCOL_VERSION, "upgraded": False,
                            "transformation": transformation}

                self._require_lease()
                marker_etag = self._write_protocol("upgrading", marker_etag)
                if manifest_version != SYNC_PROTOCOL_VERSION:
                    upgraded = dict(manifest, version=SYNC_PROTOCOL_VERSION)
                    self._require_lease()
                    self.put_manifest(upgraded, expected_etag=manifest_etag)
                self._require_lease()
                self._write_protocol("ready", marker_etag)
                self.check_protocol()
                return {"status": "ok", "protocol_version": SYNC_PROTOCOL_VERSION,
                        "upgraded": True, "from_protocol_version": manifest_version,
                        "transformation": transformation}
            finally:
                if not dry_run:
                    self.release_lock()

    # --- Manifest operations ---

    def get_manifest(self) -> dict:
        """Get the remote manifest (file inventory + hashes).

        Empty means an unused remote; existing objects without inventory require
        an explicit rebuild (legacy) or a restored manifest (version 2).
        """
        with self._protocol_errors():
            self.check_protocol()
            manifest, etag = self._read_json("manifest.json")
            if manifest is not None:
                require_matching_protocol(manifest.get("version", 1))
            else:
                self._require_empty_remote()
                manifest = {"files": {}}
            self.manifest_etag = etag
            return manifest

    def _read_manifest(self, allow_legacy_rebuild=False):
        key = self._sync_key("manifest.json")
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            manifest = json.loads(response["Body"].read().decode("utf-8"))
            self._validate_manifest_version(manifest)
            self.manifest_etag = response["ETag"]
            return manifest
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                objects = self._client.list_objects_v2(Bucket=self.bucket,
                    Prefix=self._sync_key("objects/"), MaxKeys=1)
                if objects.get("KeyCount", 0):
                    raise ValueError("Immutable sync manifest missing; restore it from backup")
                legacy = self._client.list_objects_v2(Bucket=self.bucket,
                    Prefix=self._key("storage/"), MaxKeys=1)
                if legacy.get("KeyCount", 0) and not allow_legacy_rebuild:
                    raise ValueError("Legacy sync manifest missing; rebuild inventory explicitly before syncing")
                self.manifest_etag = None
                return {"files": {}}
            raise

    @staticmethod
    def _validate_manifest_version(manifest):
        if (not isinstance(manifest, dict) or type(manifest.get("version", 1)) is not int
                or manifest.get("version", 1) not in (1, 2)):
            raise ValueError("Unsupported remote manifest version")

    def put_manifest(self, manifest: dict, *, expected_etag=_UNCONDITIONAL) -> str:
        """Publish inventory; sync passes its snapshot ETag as a compare-and-swap.

        None requires an absent manifest. Omitting the argument retains the direct
        administrative API. Conditional revisions are unique, avoiding ETag ABA
        even when a catalog is changed and later restored to identical content.
        """
        self._validate_manifest_version(manifest)
        conditions = {}
        if expected_etag is not _UNCONDITIONAL:
            conditions = {"IfMatch": expected_etag} if expected_etag else {"IfNoneMatch": "*"}
            manifest = dict(manifest, revision=uuid.uuid4().hex)
        try:
            response = self._client.put_object(
                Bucket=self.bucket, Key=self._sync_key("manifest.json"),
                Body=json.dumps(manifest, indent=2).encode("utf-8"),
                ContentType="application/json", **conditions)
        except ClientError as error:
            if self._condition_failed(error):
                raise RuntimeError("Remote manifest changed during sync; retry with current inventory") from error
            raise
        self.manifest_etag = response["ETag"]
        return self.manifest_etag

    def rebuild_manifest(self) -> dict:
        """Rebuild manifest by listing S3 bucket and computing hashes.

        Used for recovery when manifest is corrupt or missing.
        """
        current = self._read_manifest(allow_legacy_rebuild=True)
        if current.get("version") == 2:
            # Immutable hashes cannot recover filenames or deletion intent. The
            # authoritative inventory is retained, never reconstructed by listing.
            if not isinstance(current.get("files"), dict) or not isinstance(current.get("tombstones", {}), dict):
                raise ValueError("Cannot safely rebuild invalid version 2 manifest")
            for name, entry in current["files"].items():
                content = self.download_version(entry["blob"]) if entry.get("blob") else self.download_file(name)
                if content is None or not self.verify_download(content, entry["sha256"]):
                    raise ValueError(f"Cannot rebuild: missing or corrupt remote content for {name}")
            return current
        # If the manifest was lost after migration, listing legacy storage would
        # discard the immutable inventory and tombstones. Require a backup.
        objects = self._client.list_objects_v2(Bucket=self.bucket,
                                              Prefix=self._sync_key("objects/"), MaxKeys=1)
        if objects.get("KeyCount", 0):
            raise ValueError("Immutable sync objects exist; restore the version 2 manifest from backup")
        prefix = f"{self.prefix}storage/"
        files = {}
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key[len(prefix):]
                if not name.endswith(".md"):
                    continue
                # Download and hash
                response = self._client.get_object(Bucket=self.bucket, Key=key)
                content = response["Body"].read()
                sha = hashlib.sha256(content).hexdigest()
                files[name] = {
                    "sha256": sha,
                    "size": len(content),
                    "modified": obj["LastModified"].isoformat(),
                }

        manifest = {
            "files": files,
            "rebuilt": True,
            "last_sync": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.put_manifest(manifest, expected_etag=self.manifest_etag)
        return manifest

    # --- Advisory locking ---

    def _read_lock(self):
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self._sync_key("lock.json"))
            return json.loads(response["Body"].read()), response["ETag"]
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None, None
            raise

    def _write_lock(self, lease_id, **condition):
        return self._client.put_object(
            Bucket=self.bucket, Key=self._sync_key("lock.json"),
            Body=json.dumps({"machine_id": self._machine_id, "lease_id": lease_id,
                             "timestamp": time.time(), "ttl": LOCK_TTL_SECONDS}).encode(),
            ContentType="application/json", **condition)

    @staticmethod
    def _condition_failed(error):
        return error.response["Error"]["Code"] in (
            "PreconditionFailed", "ConditionalRequestConflict", "412", "409")

    def acquire_lock(self) -> bool:
        """Acquire a unique lease using an atomic S3 precondition.

        Unsupported conditional writes fail closed. A machine ID is diagnostic,
        never ownership: two processes on the same machine remain competitors.
        """
        with self._lease_mutex:
            current, etag = self._read_lock()
            if current and time.time() - current.get("timestamp", 0) < LOCK_TTL_SECONDS:
                return False
            lease_id = uuid.uuid4().hex
            try:
                self._write_lock(lease_id, **({"IfMatch": etag} if etag else {"IfNoneMatch": "*"}))
            except ClientError as error:
                if self._condition_failed(error):
                    return False
                raise
            self._lease_id = lease_id
            return True

    def renew_lock(self) -> bool:
        """Renew only the current operation's unexpired lease using compare-and-swap."""
        with self._lease_mutex:
            if self._lease_id is None:
                return False
            current, etag = self._read_lock()
            if (not current or current.get("lease_id") != self._lease_id or
                    time.time() - current.get("timestamp", 0) >= LOCK_TTL_SECONDS):
                return False
            try:
                self._write_lock(self._lease_id, IfMatch=etag)
                return True
            except ClientError as error:
                if self._condition_failed(error):
                    return False
                raise

    def release_lock(self) -> None:
        """Conditionally delete only our lease; never release a successor's lock."""
        with self._lease_mutex:
            if self._lease_id is None:
                return
            try:
                current, etag = self._read_lock()
                if current and current.get("lease_id") == self._lease_id:
                    self._client.delete_object(Bucket=self.bucket, Key=self._sync_key("lock.json"),
                                               IfMatch=etag)
            except ClientError as error:
                if not self._condition_failed(error):
                    logger.warning("Failed to release sync lease: %s", error)
            finally:
                self._lease_id = None

    def upload_version(self, content: bytes) -> str:
        """Store immutable content so failed manifest publication cannot corrupt inventory."""
        digest = hashlib.sha256(content).hexdigest()
        self._client.put_object(Bucket=self.bucket, Key=self._sync_key("objects/" + digest),
                                Body=content, ContentType="text/markdown")
        return digest

    def download_version(self, digest: str) -> bytes:
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("Invalid remote object digest")
        response = self._client.get_object(Bucket=self.bucket, Key=self._sync_key("objects/" + digest))
        return response["Body"].read()

    def check_lock(self) -> Optional[dict]:
        """Check the current lock status without acquiring.

        Returns lock info dict if locked, None if unlocked.
        """
        key = self._sync_key("lock.json")
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            lock_data = json.loads(response["Body"].read().decode("utf-8"))
            lock_time = lock_data.get("timestamp", 0)
            if time.time() - lock_time >= LOCK_TTL_SECONDS:
                return None  # Stale lock
            return lock_data
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    # --- Utilities ---

    def verify_download(self, content: bytes, expected_sha: str) -> bool:
        """Verify downloaded content against expected SHA256 hash."""
        actual = hashlib.sha256(content).hexdigest()
        return actual == expected_sha

    def check_connectivity(self) -> bool:
        """Check if we can reach the S3 bucket."""
        try:
            self._client.head_bucket(Bucket=self.bucket)
            return True
        except (ClientError, NoCredentialsError, Exception):
            return False

    @staticmethod
    def _get_machine_id() -> str:
        """Get a unique machine identifier for locking."""
        from .crypto import _get_machine_key_material
        return _get_machine_key_material()
