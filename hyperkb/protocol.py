"""Stable compatibility envelope shared by MCP, CLI and S3 synchronization."""
import hashlib
import json
import os
import tempfile
import threading

from . import __version__
from .locking import FileLock

SYNC_PROTOCOL_VERSION = 2


class ProtocolError(RuntimeError):
    """An actionable failure that blocks remote synchronization."""

    def __init__(self, status: str, message: str, remote_version=None):
        super().__init__(message)
        self.status = status
        self.remote_version = remote_version

    def as_dict(self):
        return {
            "status": self.status, "blocked": True, "scope": "remote_sync",
            "local_operations_allowed": True, "remote_sync_blocked": True,
            "message": str(self),
            "client_protocol_version": SYNC_PROTOCOL_VERSION,
            "remote_protocol_version": self.remote_version,
            "client_version": __version__,
            "upgrade_command": "hkb update apply",
            "retryable": self.status == "protocol_unverified",
        }


class ProtocolUnavailable(ProtocolError):
    """A transport outage, distinct from invalid or inaccessible metadata."""

    def __init__(self):
        super().__init__("protocol_unverified", "S3 is unreachable; its protocol could not be verified. Local recording remains available; remote sync is paused.")


class ProtocolGate:
    """Check each operation, remembering blockers across processes and restarts.

    Compatibility is never cached as permission to skip an online check. A
    previous mismatch remains visible during outages. Local-only callers opt in
    to warnings instead of errors; remote synchronization always checks strictly.
    The latch is scoped to the S3 destination, independent of credentials.
    """

    def __init__(self, config, remote=None):
        self.config = config
        self.remote = remote
        self._provided_remote = remote is not None
        self._settings = None
        self._mutex = threading.RLock()
        self.state = {"status": "unchecked", "blocked": False}

    def _target_path(self):
        target = [getattr(self.config, "sync_" + key) for key in
                  ("bucket", "prefix", "region", "endpoint_url")]
        target[1] = target[1].rstrip("/")
        digest = hashlib.sha256(json.dumps(target).encode()).hexdigest()
        return self.config.storage_dir.parent / "sync" / f"protocol-{digest}.json"

    def _refresh(self):
        if not self._provided_remote and self.config.config_path.exists():
            loaded = type(self.config).load(self.config.root)
            for key, value in vars(loaded).items():
                if key.startswith("sync_"):
                    setattr(self.config, key, value)

    def _get_remote(self):
        if self._provided_remote:
            return self.remote
        settings = tuple(getattr(self.config, "sync_" + key) for key in
                         ("bucket", "prefix", "region", "endpoint_url", "access_key", "secret_key"))
        if self.remote is None or settings != self._settings:
            from .remote import S3Remote
            self.remote = S3Remote(*settings)
            self._settings = settings
        return self.remote

    @staticmethod
    def _remember(path, error):
        # Atomic publication under the protocol lock; never store credentials.
        fd, temp = tempfile.mkstemp(prefix=".protocol-", dir=path.parent)
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(error.as_dict(), handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)

    @staticmethod
    def _previous_error(path):
        try:
            previous = json.loads(path.read_text())
        except FileNotFoundError:
            return None
        except (ValueError, OSError):
            return ProtocolError("protocol_unverified", "Saved protocol state is unreadable. Reconnect to S3 to verify compatibility.")
        if not isinstance(previous, dict):
            return ProtocolError("protocol_unverified", "Saved protocol state is invalid. Reconnect to S3 to verify compatibility.")
        version = previous.get("remote_protocol_version")
        if version is not None:
            try:
                require_matching_protocol(version)
            except ProtocolError as error:
                return error
        # Even an upgraded binary must verify the current remote before clearing
        # a latch. The bucket may have advanced again while it was disconnected.
        return ProtocolError("protocol_unverified", "A previous protocol check blocked remote sync. Keep recording locally; reconnect to S3 to verify compatibility before syncing.", version)

    def check(self, *, allow_offline=True, allow_local=False):
        with self._mutex:
            try:
                self._refresh()
                if not self.config.sync_enabled:
                    self.state = {"status": "local", "blocked": False,
                                  "client_protocol_version": SYNC_PROTOCOL_VERSION}
                    return self.state
                if not self.config.sync_bucket:
                    raise ProtocolError("protocol_unverified", "Sync is enabled without an S3 bucket. Local recording remains available; configure the bucket before syncing.")
                path = self._target_path()
                with FileLock(path.with_suffix(".lock"), name="protocol"):
                    previous = self._previous_error(path)
                    try:
                        result = self._get_remote().check_protocol()
                        require_matching_protocol(result.get("protocol_version"))
                    except ProtocolUnavailable:
                        if previous is not None:
                            raise previous
                        if not allow_offline:
                            raise
                        self.state = {"status": "offline", "blocked": True,
                                      "scope": "remote_sync", "local_operations_allowed": True,
                                      "remote_sync_blocked": True,
                                      "client_protocol_version": SYNC_PROTOCOL_VERSION,
                                      "message": "S3 is unreachable. Local recording remains available; remote sync is paused until its protocol can be verified."}
                        return self.state
                    except ProtocolError as error:
                        self._remember(path, error)
                        raise
                    path.unlink(missing_ok=True)
                    self.state = {"status": "compatible", "blocked": False,
                                  "client_protocol_version": SYNC_PROTOCOL_VERSION,
                                  **result}
                    return self.state
            except ProtocolError as error:
                self.state = error.as_dict()
                if allow_local:
                    return self.state
                raise
            except Exception as error:
                # Do not echo provider exceptions: endpoints and credentials can
                # appear in their strings. An unknown failure never enables use.
                failure = ProtocolError("protocol_unverified", "The S3 protocol could not be verified. Check sync configuration, credentials, and local state permissions, then retry.")
                self.state = failure.as_dict()
                if allow_local:
                    return self.state
                raise failure from error


def require_matching_protocol(version):
    """Require exact protocol equality; never guess at a malformed envelope."""
    if type(version) is not int or version < 1:
        raise ProtocolError("protocol_unverified", "The S3 store has an invalid protocol version. Local recording remains available; remote sync is blocked until its protocol can be verified.")
    if version > SYNC_PROTOCOL_VERSION:
        raise ProtocolError("upgrade_required", f"HyperKB update required: the S3 store uses protocol {version}, but this client supports protocol {SYNC_PROTOCOL_VERSION}. Remote sync is blocked; keep recording locally. Run hkb update apply, restart this MCP client, then preview sync to reconcile pending local changes.", version)
    if version < SYNC_PROTOCOL_VERSION:
        raise ProtocolError("store_upgrade_required", f"The S3 store uses protocol {version}, but this client uses protocol {SYNC_PROTOCOL_VERSION}. Remote sync is blocked; keep recording locally. Upgrade clients without protocol checks first, preview hkb sync upgrade-protocol --dry-run, then migrate the store with hkb sync upgrade-protocol and preview sync to reconcile pending changes.", version)
    return version
