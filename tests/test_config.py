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
