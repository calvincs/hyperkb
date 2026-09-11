"""Tests for hyperkb.cli — CliRunner tests for admin commands (init, config)."""

import json
import os
import pytest
from click.testing import CliRunner

from hyperkb.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def initialized_kb(tmp_path, runner, monkeypatch):
    """Initialize a KB at a fake home dir so load() finds it as the global KB."""
    monkeypatch.setenv("HOME", str(tmp_path))
    result = runner.invoke(cli, ["init"])
    assert result.exit_code == 0, result.output
    yield tmp_path


class TestVersion:
    def test_version(self, runner):
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "version" in result.output.lower()


class TestInit:
    def test_init(self, tmp_path, runner):
        result = runner.invoke(cli, ["init", "--path", str(tmp_path)])
        assert result.exit_code == 0
        assert "initialized" in result.output.lower()
        assert (tmp_path / ".hkb" / "config.json").exists()


class TestConfig:
    def test_config_view(self, initialized_kb, runner):
        result = runner.invoke(cli, ["config", "rg_weight"])
        assert result.exit_code == 0
        assert "0.5" in result.output

    def test_config_set(self, initialized_kb, runner):
        result = runner.invoke(cli, ["config", "rg_weight", "0.5"])
        assert result.exit_code == 0
        assert "0.5" in result.output

    def test_config_unknown_key(self, initialized_kb, runner):
        result = runner.invoke(cli, ["config", "nonexistent_key"])
        assert result.exit_code != 0

    def test_config_set_flag_hidden_prompt(self, initialized_kb, runner):
        """--set on a sensitive field prompts with hidden input."""
        result = runner.invoke(
            cli,
            ["config", "sync_access_key", "--set"],
            input="sk-test-key-1234\n",
        )
        assert result.exit_code == 0
        assert "sync_access_key" in result.output
        # Value should be masked in output
        assert "1234" in result.output
        assert "sk-test-key-1234" not in result.output

    def test_config_set_flag_ignores_positional_value(self, initialized_kb, runner):
        """--set with a positional value still prompts (ignores the positional)."""
        result = runner.invoke(
            cli,
            ["config", "sync_access_key", "should-be-ignored", "--set"],
            input="sk-from-prompt-5678\n",
        )
        assert result.exit_code == 0
        assert "5678" in result.output

    def test_config_sensitive_cli_arg_warns(self, initialized_kb, runner):
        """Passing sensitive value as CLI arg prints a shell history warning."""
        result = runner.invoke(
            cli,
            ["config", "sync_access_key", "sk-test-warn-9999"],
        )
        assert result.exit_code == 0
        assert "shell history" in result.output

    def test_config_set_flag_on_non_sensitive_errors(self, initialized_kb, runner):
        """--set on a non-sensitive field without a value errors."""
        result = runner.invoke(cli, ["config", "rg_weight", "--set"])
        assert result.exit_code != 0
        assert "sensitive" in result.output

    def test_config_set_non_sensitive_with_value_works(self, initialized_kb, runner):
        """--set on a non-sensitive field with a value works normally."""
        result = runner.invoke(cli, ["config", "rg_weight", "0.3", "--set"])
        assert result.exit_code == 0
        assert "0.3" in result.output


class TestRecovery:
    def test_reindex_missing_database_preserves_configuration(self, initialized_kb, runner):
        from hyperkb.config import KBConfig
        from hyperkb.store import KnowledgeStore
        cfg = KBConfig.load(str(initialized_kb))
        cfg.set_value("rg_weight", "0.7")
        store = KnowledgeStore(cfg)
        store.db.connect()
        store.create_file("recover.notes", "Recovery notes", ["recovery"])
        store.add_entry("Recover this durable knowledge", file_name="recover.notes")
        store.close()
        previous = cfg.config_path.read_bytes()
        cfg.db_path.unlink()
        missing = runner.invoke(cli, ["doctor"])
        assert missing.exit_code != 0
        assert "Index is missing" in missing.output
        repaired = runner.invoke(cli, ["reindex"])
        assert repaired.exit_code == 0, repaired.output
        assert cfg.config_path.read_bytes() == previous
        healthy = runner.invoke(cli, ["doctor"])
        assert healthy.exit_code == 0, healthy.output
        store = KnowledgeStore(cfg)
        store.db.connect()
        assert len(store.search("durable", mode="bm25")) == 1
        store.close()

    def test_invalid_boolean_does_not_modify_config(self, initialized_kb, runner):
        from hyperkb.config import KBConfig
        cfg = KBConfig.load(str(initialized_kb))
        previous = cfg.config_path.read_bytes()
        result = runner.invoke(cli, ["config", "sync_enabled", "maybe"])
        assert result.exit_code != 0
        assert "true or false" in result.output
        assert cfg.config_path.read_bytes() == previous


def test_reindex_preserves_corrupt_index_for_diagnosis(initialized_kb, runner):
    from hyperkb.config import KBConfig
    cfg = KBConfig.load(str(initialized_kb))
    evidence = b"corrupt index evidence"
    cfg.db_path.write_bytes(evidence)
    previous = cfg.config_path.read_bytes()
    diagnosed = runner.invoke(cli, ["doctor"])
    assert diagnosed.exit_code != 0
    assert cfg.db_path.read_bytes() == evidence
    result = runner.invoke(cli, ["reindex"])
    assert result.exit_code == 0, result.output
    quarantined = list(cfg.hkb_dir.glob("index.db.corrupt-*"))
    assert any(path.read_bytes() == evidence for path in quarantined)
    assert cfg.config_path.read_bytes() == previous


def test_doctor_reports_old_schema_without_migrating(tmp_path, runner):
    import sqlite3
    from hyperkb.config import KBConfig
    cfg = KBConfig(root=str(tmp_path))
    cfg.save()
    with sqlite3.connect(cfg.db_path) as conn:
        conn.execute("CREATE TABLE entries (file_name TEXT, epoch INTEGER, content TEXT)")
        conn.execute("INSERT INTO entries VALUES ('old.notes', 1, 'Original old index')")
    before = cfg.db_path.read_bytes()
    config_before = cfg.config_path.read_bytes()
    files_before = sorted(path.name for path in cfg.hkb_dir.iterdir())
    result = runner.invoke(cli, ["doctor", "--path", str(tmp_path)])
    assert result.exit_code != 0
    assert "schema migration needed" in result.output
    assert "entries.author" in result.output
    assert cfg.db_path.read_bytes() == before
    assert cfg.config_path.read_bytes() == config_before
    assert sorted(path.name for path in cfg.hkb_dir.iterdir()) == files_before
    with sqlite3.connect(cfg.db_path) as conn:
        assert [row[1] for row in conn.execute("PRAGMA table_info(entries)")] == ["file_name", "epoch", "content"]


def test_doctor_database_adapter_rejects_writes(initialized_kb):
    import sqlite3
    from hyperkb.cli import _ReadOnlyDatabase
    from hyperkb.config import KBConfig
    db = _ReadOnlyDatabase(KBConfig.load(str(initialized_kb)))
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.connect().execute("CREATE TABLE forbidden (value TEXT)")
    finally:
        db.close()
