"""The gate preserves local work without letting an outage erase incompatibility."""
import json

import pytest

from hyperkb.config import KBConfig
from hyperkb.protocol import (
    ProtocolError, ProtocolGate, ProtocolUnavailable, require_matching_protocol,
)


class Remote:
    version = 2
    failure = None
    calls = 0

    def check_protocol(self):
        self.calls += 1
        if self.failure:
            raise self.failure
        require_matching_protocol(self.version)
        return {"protocol_version": self.version, "marker_present": True}


def config(tmp_path, **kwargs):
    return KBConfig(root=str(tmp_path), sync_enabled=True, sync_bucket="protocol-test", **kwargs)


def test_every_check_revalidates_and_local_work_survives_mismatch(tmp_path):
    remote = Remote()
    gate = ProtocolGate(config(tmp_path), remote=remote)
    assert gate.check()["status"] == "compatible"
    remote.version = 3
    notice = gate.check(allow_local=True)
    assert notice["status"] == "upgrade_required"
    assert notice["local_operations_allowed"] is True
    assert notice["remote_sync_blocked"] is True
    assert remote.calls == 2
    with pytest.raises(ProtocolError, match="update required"):
        gate.check(allow_offline=False)


def test_restart_and_new_client_cannot_clear_detected_mismatch_offline(tmp_path):
    cfg = config(tmp_path)
    remote = Remote()
    remote.version = 3
    gate = ProtocolGate(cfg, remote=remote)
    gate.check(allow_local=True)
    remote.failure = ProtocolUnavailable()
    restarted = ProtocolGate(cfg, remote=remote)
    assert restarted.check(allow_local=True)["status"] == "upgrade_required"
    with pytest.raises(ProtocolError) as error:
        restarted.check(allow_offline=False)
    assert error.value.remote_version == 3
    remote.failure = None
    remote.version = 2
    assert restarted.check()["status"] == "compatible"
    remote.failure = ProtocolUnavailable()
    assert ProtocolGate(cfg, remote=remote).check(allow_local=True)["status"] == "offline"


def test_new_offline_client_can_record_but_cannot_sync(tmp_path):
    remote = Remote()
    remote.failure = ProtocolUnavailable()
    gate = ProtocolGate(config(tmp_path), remote=remote)
    assert gate.check(allow_local=True)["local_operations_allowed"]
    with pytest.raises(ProtocolUnavailable):
        gate.check(allow_offline=False)


def test_latch_is_destination_specific_and_excludes_credentials(tmp_path):
    remote = Remote()
    remote.version = 3
    cfg = config(tmp_path, sync_access_key="private-access", sync_secret_key="private-secret")
    first = ProtocolGate(cfg, remote=remote)
    first.check(allow_local=True)
    saved = first._target_path().read_text()
    assert "private-access" not in saved and "private-secret" not in saved
    cfg.sync_access_key = "replacement"
    remote.failure = ProtocolUnavailable()
    assert ProtocolGate(cfg, remote=remote).check(allow_local=True)["status"] == "upgrade_required"
    cfg.sync_prefix = "other/"
    assert ProtocolGate(cfg, remote=remote).check(allow_local=True)["status"] == "offline"


def test_upgraded_binary_still_needs_online_verification(tmp_path):
    remote = Remote()
    gate = ProtocolGate(config(tmp_path), remote=remote)
    gate.check()
    path = gate._target_path()
    path.write_text(json.dumps({"remote_protocol_version": 2}))
    remote.failure = ProtocolUnavailable()
    assert gate.check(allow_local=True)["status"] == "protocol_unverified"
    with pytest.raises(ProtocolError):
        gate.check(allow_offline=False)


@pytest.mark.parametrize("version", [True, False, "2", None, 2.0, -1, 0, {}, []])
def test_invalid_protocol_is_never_treated_as_compatible(version):
    with pytest.raises(ProtocolError):
        require_matching_protocol(version)


def test_config_refresh_changes_target_without_restarting(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg.save()
    remote = Remote()
    monkeypatch.setattr("hyperkb.remote.S3Remote", lambda *args: remote)
    gate = ProtocolGate(cfg)
    remote.version = 3
    assert gate.check(allow_local=True)["status"] == "upgrade_required"
    changed = KBConfig.load(str(tmp_path))
    changed.sync_prefix = "new/"
    changed.save()
    remote.failure = ProtocolUnavailable()
    assert gate.check(allow_local=True)["status"] == "offline"
    assert cfg.sync_prefix == "new/"


def test_unknown_provider_error_does_not_leak_credentials(tmp_path):
    remote = Remote()
    remote.failure = RuntimeError("SECRET https://user:password@example.invalid")
    gate = ProtocolGate(config(tmp_path), remote=remote)
    notice = gate.check(allow_local=True)
    assert "SECRET" not in json.dumps(notice)
    assert "password" not in json.dumps(notice)
    assert notice["remote_sync_blocked"]
    with pytest.raises(ProtocolError):
        gate.check(allow_offline=False)
