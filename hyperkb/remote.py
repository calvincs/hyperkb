"""S3-compatible remote storage for hyperkb sync.

Handles upload, download, list, manifest management, and advisory locking.
Uses boto3 directly with graceful degradation if not installed.

S3 layout:
    s3://bucket/<prefix>/
        _sync/
            manifest.json    - file inventory + SHA256 hashes
            lock.json        - advisory lock (machine_id + TTL)
        storage/
            *.md             - mirrors ~/.hkb/storage/ exactly
"""

import hashlib
import json
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

BOTO3_AVAILABLE = False
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    pass

LOCK_TTL_SECONDS = 300  # 5 minutes


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

        # Build boto3 client kwargs
        kwargs = {}
        if region:
            kwargs["region_name"] = region
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key

        self._client = boto3.client("s3", **kwargs)

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

    # --- Manifest operations ---

    def get_manifest(self) -> dict:
        """Get the remote manifest (file inventory + hashes).

        Returns empty dict with "files": {} if manifest doesn't exist.
        """
        key = self._sync_key("manifest.json")
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return {"files": {}}
            raise

    def put_manifest(self, manifest: dict) -> None:
        """Write the manifest to S3."""
        key = self._sync_key("manifest.json")
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    def rebuild_manifest(self) -> dict:
        """Rebuild manifest by listing S3 bucket and computing hashes.

        Used for recovery when manifest is corrupt or missing.
        """
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
        self.put_manifest(manifest)
        return manifest

    # --- Advisory locking ---

    def acquire_lock(self) -> bool:
        """Acquire an advisory lock on the S3 sync.

        Uses a lock.json file with machine_id and TTL.
        Automatically breaks stale locks (older than TTL).

        Returns True if lock was acquired, False if another machine holds it.
        """
        key = self._sync_key("lock.json")

        # Check for existing lock
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            lock_data = json.loads(response["Body"].read().decode("utf-8"))
            lock_time = lock_data.get("timestamp", 0)
            lock_machine = lock_data.get("machine_id", "")

            # Same machine can reacquire
            if lock_machine == self._machine_id:
                pass  # Fall through to acquire
            # Check if lock is stale (older than TTL)
            elif time.time() - lock_time < LOCK_TTL_SECONDS:
                logger.warning(
                    "Sync lock held by %s (age: %ds)",
                    lock_machine,
                    int(time.time() - lock_time),
                )
                return False
            else:
                logger.info("Breaking stale lock from %s", lock_machine)
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                raise

        # Write our lock
        lock_data = {
            "machine_id": self._machine_id,
            "timestamp": time.time(),
            "ttl": LOCK_TTL_SECONDS,
        }
        self._client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(lock_data).encode("utf-8"),
            ContentType="application/json",
        )
        return True

    def release_lock(self) -> None:
        """Release the advisory lock."""
        key = self._sync_key("lock.json")
        try:
            self._client.delete_object(Bucket=self.bucket, Key=key)
        except ClientError:
            logger.debug("Failed to release lock (may already be released)")

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
