"""Tests for hyperkb.config — save/load, defaults."""

import json
import pytest
from pathlib import Path

from hyperkb.config import KBConfig, HKB_DIR, CONFIG_FILENAME, DB_FILENAME


class TestKBConfigDefaults:
    def test_default_values(self):
        cfg = KBConfig()
        assert cfg.root == ""
        assert cfg.rg_weight == 0.5
        assert cfg.bm25_weight == 0.5
        assert cfg.route_confidence_threshold == 0.6
        assert cfg.rg_timeout == 10.0
        assert cfg.max_entry_size == 1_048_576

    def test_hkb_dir_property(self, tmp_path):
        cfg = KBConfig(root=str(tmp_path))
        assert cfg.hkb_dir == tmp_path / HKB_DIR

    def test_db_path_property(self, tmp_path):
        cfg = KBConfig(root=str(tmp_path))
        assert cfg.db_path == tmp_path / HKB_DIR / DB_FILENAME

    def test_config_path_property(self, tmp_path):
        cfg = KBConfig(root=str(tmp_path))
        assert cfg.config_path == tmp_path / HKB_DIR / CONFIG_FILENAME


class TestKBConfigSaveLoad:
    def test_save_creates_dir_and_file(self, tmp_path):
        cfg = KBConfig(root=str(tmp_path))
        cfg.save()
        assert cfg.config_path.exists()
        data = json.loads(cfg.config_path.read_text())
        assert data["root"] == str(tmp_path)

    def test_load_from_root(self, tmp_path):
        cfg = KBConfig(root=str(tmp_path))
        cfg.save()
        loaded = KBConfig.load(str(tmp_path))
        assert loaded.root == str(tmp_path)

    def test_load_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No hyperkb found"):
            KBConfig.load(str(tmp_path))

    def test_round_trip_preserves_values(self, tmp_path):
        cfg = KBConfig(
            root=str(tmp_path),
            rg_weight=0.5,
            bm25_weight=0.5,
            route_confidence_threshold=0.7,
        )
        cfg.save()
        loaded = KBConfig.load(str(tmp_path))
        assert loaded.rg_weight == 0.5
        assert loaded.bm25_weight == 0.5
        assert loaded.route_confidence_threshold == 0.7

    def test_round_trip_new_fields(self, tmp_path):
        cfg = KBConfig(
            root=str(tmp_path),
            rg_timeout=15.0,
            max_entry_size=500_000,
        )
        cfg.save()
        loaded = KBConfig.load(str(tmp_path))
        assert loaded.rg_timeout == 15.0
        assert loaded.max_entry_size == 500_000


class TestLoadDefaultsToGlobal:
    def test_load_defaults_to_global(self, tmp_path, monkeypatch):
        """load() without root should load from ~/.hkb/."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))
        cfg = KBConfig(root=str(fake_home))
        cfg.save()
        loaded = KBConfig.load()
        assert loaded.root == str(fake_home)

    def test_load_no_global_raises(self, tmp_path, monkeypatch):
        """load() without root should raise if no global KB."""
        fake_home = tmp_path / "emptyhome"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))
        with pytest.raises(FileNotFoundError, match="No hyperkb found"):
            KBConfig.load()


@pytest.mark.parametrize("key,value", [
    ("sync_enabled", "perhaps"), ("sync_interval", "0"),
    ("rg_weight", "nan"), ("bm25_weight", "-1"), ("root", "/elsewhere"),
])
def test_invalid_setting_preserves_config(tmp_path, key, value):
    cfg = KBConfig(root=str(tmp_path))
    cfg.save()
    previous = cfg.config_path.read_bytes()
    with pytest.raises(ValueError):
        cfg.set_value(key, value)
    assert cfg.config_path.read_bytes() == previous
    assert KBConfig.load(str(tmp_path)) == cfg


def test_boolean_setting_parsing(tmp_path):
    cfg = KBConfig(root=str(tmp_path))
    cfg.set_value("sync_enabled", "true")
    assert KBConfig.load(str(tmp_path)).sync_enabled is True
    cfg.set_value("sync_enabled", "false")
    assert KBConfig.load(str(tmp_path)).sync_enabled is False


def test_atomic_save_preserves_previous_config_on_replace_failure(tmp_path, monkeypatch):
    cfg = KBConfig(root=str(tmp_path))
    cfg.save()
    previous = cfg.config_path.read_bytes()
    def fail(*args):
        raise OSError("injected replacement failure")
    monkeypatch.setattr("hyperkb.config.os.replace", fail)
    with pytest.raises(OSError):
        cfg.set_value("sync_interval", "120")
    assert cfg.config_path.read_bytes() == previous
    assert cfg.sync_interval == 60
    assert not list(cfg.hkb_dir.glob(".config-*.tmp"))


def test_moved_kb_uses_actual_location(tmp_path):
    cfg = KBConfig(root=str(tmp_path / "old"))
    cfg.save()
    new = tmp_path / "new"
    cfg.hkb_dir.parent.rename(new)
    loaded = KBConfig.load(str(new))
    assert loaded.root == str(new)
    assert loaded.config_path.exists()


def test_two_clients_preserve_each_others_config_edits(tmp_path):
    first = KBConfig(root=str(tmp_path))
    first.save()
    second = KBConfig.load(str(tmp_path))
    first.set_value("sync_interval", "120")
    second.set_value("rg_weight", "0.8")
    loaded = KBConfig.load(str(tmp_path))
    assert loaded.sync_interval == 120
    assert loaded.rg_weight == 0.8


def test_client_environment_does_not_overwrite_shared_provenance(tmp_path, monkeypatch):
    cfg = KBConfig(root=str(tmp_path), default_source="shared-default")
    cfg.save()
    monkeypatch.setenv("HKB_SOURCE", "client-a")
    client = KBConfig.load(str(tmp_path))
    client.set_value("sync_interval", "120")
    assert client.default_source == "client-a"
    assert KBConfig.load(str(tmp_path), apply_env=False).default_source == "shared-default"


def test_foreign_credentials_use_env_before_decryption_and_survive_setting(tmp_path, monkeypatch):
    pytest.importorskip("cryptography")
    from hyperkb.crypto import decrypt_value
    monkeypatch.setattr("hyperkb.crypto._get_machine_key_material", lambda: "source-machine")
    cfg = KBConfig(root=str(tmp_path), sync_access_key="stored-access", sync_secret_key="stored-secret")
    cfg.save()
    original = json.loads(cfg.config_path.read_text())
    monkeypatch.setattr("hyperkb.crypto._get_machine_key_material", lambda: "destination-machine")
    monkeypatch.setenv("HKB_SYNC_ACCESS_KEY", "environment-access")
    monkeypatch.setenv("HKB_SYNC_SECRET_KEY", "environment-secret")
    loaded = KBConfig.load(str(tmp_path))
    assert loaded.sync_access_key == "environment-access"
    assert loaded.sync_secret_key == "environment-secret"
    loaded.set_value("sync_interval", "120")
    updated = json.loads(cfg.config_path.read_text())
    assert updated["sync_access_key"] == original["sync_access_key"]
    assert updated["sync_secret_key"] == original["sync_secret_key"]
    assert "environment-access" not in cfg.config_path.read_text()
    assert "environment-secret" not in cfg.config_path.read_text()
    assert loaded.sync_interval == 120
    # An explicit credential edit changes only that persisted credential, while
    # the environment retains precedence in the current client's effective config.
    loaded.set_value("sync_secret_key", "replacement-secret")
    updated = json.loads(cfg.config_path.read_text())
    assert updated["sync_access_key"] == original["sync_access_key"]
    assert decrypt_value(updated["sync_secret_key"]) == "replacement-secret"
    assert loaded.sync_secret_key == "environment-secret"


@pytest.mark.parametrize("bad", [None, [], {}, 42, False])
def test_env_override_does_not_hide_malformed_credential_type(bad, monkeypatch):
    monkeypatch.setenv("HKB_SYNC_SECRET_KEY", "valid-env-secret")
    with pytest.raises(ValueError, match="sync_secret_key must be str"):
        KBConfig._from_dict({"sync_secret_key": bad})


def test_foreign_credentials_without_override_still_fail(tmp_path, monkeypatch):
    pytest.importorskip("cryptography")
    from cryptography.fernet import InvalidToken
    monkeypatch.delenv("HKB_SYNC_SECRET_KEY", raising=False)
    monkeypatch.setattr("hyperkb.crypto._get_machine_key_material", lambda: "source-machine")
    cfg = KBConfig(root=str(tmp_path), sync_secret_key="stored-secret")
    cfg.save()
    monkeypatch.setattr("hyperkb.crypto._get_machine_key_material", lambda: "destination-machine")
    with pytest.raises(InvalidToken):
        KBConfig.load(str(tmp_path))
