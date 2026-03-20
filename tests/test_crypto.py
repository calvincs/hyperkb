"""Tests for hyperkb.crypto — encryption, decryption, masking, env overrides."""

import json
import os
import pytest

from hyperkb.crypto import (
    encrypt_value, decrypt_value, mask_value, is_sensitive_field,
    _get_machine_key_material, SENSITIVE_FIELDS, _ENC_PREFIX,
)
from hyperkb.config import KBConfig


class TestSensitiveFields:
    def test_sync_access_key_is_sensitive(self):
        assert is_sensitive_field("sync_access_key")

    def test_sync_secret_key_is_sensitive(self):
        assert is_sensitive_field("sync_secret_key")

    def test_non_sensitive_fields(self):
        assert not is_sensitive_field("rg_weight")
        assert not is_sensitive_field("bm25_weight")
        assert not is_sensitive_field("root")
        assert not is_sensitive_field("sync_bucket")


class TestMachineKeyMaterial:
    def test_returns_nonempty_string(self):
        material = _get_machine_key_material()
        assert isinstance(material, str)
        assert len(material) > 0


class TestEncryptDecrypt:
    def test_roundtrip(self):
        original = "sk-ant-api03-test-key-1234567890"
        encrypted = encrypt_value(original)
        assert encrypted.startswith(_ENC_PREFIX)
        assert original not in encrypted
        decrypted = decrypt_value(encrypted)
        assert decrypted == original

    def test_empty_value_passthrough(self):
        assert encrypt_value("") == ""
        assert decrypt_value("") == ""

    def test_already_encrypted_passthrough(self):
        original = "sk-test-key"
        encrypted = encrypt_value(original)
        # Encrypting again should be a no-op
        double_encrypted = encrypt_value(encrypted)
        assert double_encrypted == encrypted

    def test_plaintext_decrypt_passthrough(self):
        """Plaintext values (no enc: prefix) pass through unchanged."""
        assert decrypt_value("sk-plain-key") == "sk-plain-key"

    def test_none_like_passthrough(self):
        assert encrypt_value("") == ""
        assert decrypt_value("") == ""


class TestMasking:
    def test_mask_normal_value(self):
        assert mask_value("sk-ant-1234") == "****1234"

    def test_mask_short_value(self):
        assert mask_value("abc") == "****"
        assert mask_value("abcd") == "****"

    def test_mask_empty(self):
        assert mask_value("") == "(not set)"

    def test_mask_encrypted_value(self):
        encrypted = encrypt_value("sk-ant-test-key-5678")
        masked = mask_value(encrypted)
        assert masked == "****5678"


class TestConfigEncryption:
    def test_save_encrypts_sync_access_key(self, tmp_path):
        cfg = KBConfig(
            root=str(tmp_path),
            sync_access_key="AKIAIOSFODNN7EXAMPLE",
        )
        cfg.save()

        raw = json.loads(cfg.config_path.read_text())
        # Should be encrypted on disk
        assert raw["sync_access_key"].startswith(_ENC_PREFIX)
        assert "AKIAIOSFODNN7EXAMPLE" not in raw["sync_access_key"]

    def test_load_decrypts_sync_access_key(self, tmp_path):
        cfg = KBConfig(
            root=str(tmp_path),
            sync_access_key="AKIAIOSFODNN7EXAMPLE",
        )
        cfg.save()

        loaded = KBConfig.load(str(tmp_path))
        assert loaded.sync_access_key == "AKIAIOSFODNN7EXAMPLE"

    def test_save_empty_key_no_encryption(self, tmp_path):
        cfg = KBConfig(root=str(tmp_path))
        cfg.save()

        raw = json.loads(cfg.config_path.read_text())
        assert raw["sync_access_key"] == ""

    def test_env_var_override(self, tmp_path, monkeypatch):
        cfg = KBConfig(
            root=str(tmp_path),
            sync_access_key="file-access-key",
        )
        cfg.save()

        monkeypatch.setenv("HKB_SYNC_ACCESS_KEY", "env-access-key")
        loaded = KBConfig.load(str(tmp_path))
        assert loaded.sync_access_key == "env-access-key"

    def test_backward_compat_plaintext_load(self, tmp_path):
        """Old configs with plaintext keys should still load."""
        hkb_dir = tmp_path / ".hkb"
        hkb_dir.mkdir()
        config_data = {
            "root": str(tmp_path),
            "rg_weight": 0.5,
            "bm25_weight": 0.5,
            "route_confidence_threshold": 0.6,
            "sync_access_key": "plaintext-access-key",
            "sync_secret_key": "plaintext-secret-key",
        }
        (hkb_dir / "config.json").write_text(json.dumps(config_data))

        loaded = KBConfig.load(str(tmp_path))
        assert loaded.sync_access_key == "plaintext-access-key"
        assert loaded.sync_secret_key == "plaintext-secret-key"
