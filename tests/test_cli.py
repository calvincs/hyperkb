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


@pytest.fixture
def synced_config(initialized_kb):
    from hyperkb.config import KBConfig
    cfg = KBConfig.load(str(initialized_kb))
    cfg.set_value("sync_bucket", "test-protocol-bucket")
    cfg.set_value("sync_enabled", "true")
    return cfg


@pytest.fixture
def protocol_gate_stub(monkeypatch):
    import hyperkb.protocol as protocol
    class Gate:
        error = None
        calls = 0
        def __init__(self, config):
            self.config = config
        def check(self, *, allow_local=False):
            Gate.calls += 1
            if self.config.sync_enabled and Gate.error:
                if allow_local:
                    return Gate.error.as_dict()
                raise Gate.error
            return {"protocol_version": 2, "marker_present": True, "blocked": False}
    monkeypatch.setattr(protocol, "ProtocolGate", Gate, raising=False)
    return Gate


@pytest.mark.parametrize("command", ["init", "reindex"])
def test_protocol_mismatch_warns_but_allows_local_cli_mutation(synced_config, runner, protocol_gate_stub, command):
    from hyperkb.protocol import ProtocolError
    protocol_gate_stub.error = ProtocolError("upgrade_required", "Run hkb update apply and restart this client.", 3)
    (synced_config.storage_dir / "pending.notes.md").write_text("# Local pending source survives\n")
    pending = (synced_config.storage_dir / "pending.notes.md").read_bytes()
    result = runner.invoke(cli, [command, "--path", synced_config.root])
    assert result.exit_code == 0, result.output
    assert "upgrade_required" in result.output
    assert "hkb update apply" in result.output
    assert "Local work will continue" in result.output
    assert (synced_config.storage_dir / "pending.notes.md").read_bytes() == pending


def test_protocol_diagnostic_and_config_escape_routes(synced_config, runner, protocol_gate_stub):
    from hyperkb.protocol import ProtocolError
    protocol_gate_stub.error = ProtocolError("store_upgrade_required", "Run hkb sync upgrade-protocol.", 1)
    result = runner.invoke(cli, ["sync", "status", "--path", synced_config.root])
    assert result.exit_code != 0
    assert "store_upgrade_required" in result.output
    assert "hkb sync upgrade-protocol" in result.output
    assert "Git:" in result.output
    assert runner.invoke(cli, ["doctor", "--path", synced_config.root]).exit_code == 0
    result = runner.invoke(cli, ["config", "sync_enabled", "false", "--path", synced_config.root])
    assert result.exit_code == 0
    assert runner.invoke(cli, ["reindex", "--path", synced_config.root]).exit_code == 0


def test_cli_protocol_upgrade_uses_explicit_remote_migration(synced_config, runner, monkeypatch, protocol_gate_stub):
    from importlib import import_module
    from types import SimpleNamespace
    from hyperkb.protocol import ProtocolError
    module = import_module("hyperkb.cli")
    protocol_gate_stub.error = ProtocolError("store_upgrade_required", "Migrate the old protocol", 1)
    calls = []
    def migrate(*, dry_run=False):
        calls.append(dry_run)
        return {"status": "dry_run" if dry_run else "ok", "protocol_version": 2, "from_protocol_version": 1}
    monkeypatch.setattr(module, "_configured_remote", lambda config: SimpleNamespace(upgrade_protocol=migrate))
    result = runner.invoke(cli, ["sync", "upgrade-protocol", "--path", synced_config.root])
    assert result.exit_code == 0, result.output
    assert calls == [False]
    assert '"protocol_version": 2' in result.output
    assert "legacy clients" in result.output
    assert protocol_gate_stub.calls == 0


@pytest.fixture
def update_repositories(tmp_path, monkeypatch):
    """A real local upstream/checkout; package installation alone is simulated."""
    import subprocess
    from importlib import import_module
    module = import_module("hyperkb.cli")
    upstream = tmp_path / "upstream"
    checkout = tmp_path / "checkout"
    upstream.mkdir()
    def git(path, *args):
        return subprocess.run(["git", "-C", str(path), *args], check=True,
                              capture_output=True, text=True).stdout.strip()
    git(upstream, "init", "--initial-branch=main")
    git(upstream, "config", "user.email", "test@example.invalid")
    git(upstream, "config", "user.name", "Updater Test")
    (upstream / "source.txt").write_text("first revision\n")
    git(upstream, "add", "source.txt")
    git(upstream, "commit", "-m", "Initial version")
    git(upstream, "tag", "v1.0")
    subprocess.run(["git", "clone", str(upstream), str(checkout)], check=True, capture_output=True)
    git(checkout, "config", "user.email", "test@example.invalid")
    git(checkout, "config", "user.name", "Updater Test")
    monkeypatch.setattr(module, "_find_repo_dir", lambda: checkout)
    monkeypatch.setattr(module, "__version__", "1.0")
    logs = []
    monkeypatch.setattr(module, "_log_update", logs.append)
    original_run = subprocess.run
    installations = []
    failure = [False]
    def simulated_install(args, **kwargs):
        if len(args) > 3 and args[1:4] == ["-m", "pip", "install"]:
            installations.append(args)
            if failure[0]:
                raise subprocess.CalledProcessError(1, args)
            return subprocess.CompletedProcess(args, 0)
        return original_run(args, **kwargs)
    monkeypatch.setattr(subprocess, "run", simulated_install)
    return upstream, checkout, git, installations, logs, failure


def test_update_applies_new_commits_with_unchanged_tag(runner, update_repositories):
    upstream, checkout, git, installations, logs, failure = update_repositories
    (upstream / "source.txt").write_text("protocol compatibility fix\n")
    git(upstream, "commit", "-am", "Protocol compatibility fix without a new tag")
    checked = runner.invoke(cli, ["update", "check"])
    assert checked.exit_code == 0, checked.output
    assert "Source update available" in checked.output
    result = runner.invoke(cli, ["update", "apply"])
    assert result.exit_code == 0, result.output
    assert git(checkout, "rev-parse", "HEAD") == git(upstream, "rev-parse", "HEAD")
    assert (checkout / "source.txt").read_text() == "protocol compatibility fix\n"
    assert len(installations) == 1
    assert "Restart your connected MCP clients" in result.output
    assert logs


def test_update_reinstalls_at_same_commit_and_reports_install_failure(runner, update_repositories):
    upstream, checkout, git, installations, logs, failure = update_repositories
    failure[0] = True
    failed = runner.invoke(cli, ["update", "apply"])
    assert failed.exit_code != 0
    assert "package installation failed" in failed.output
    assert not logs
    failure[0] = False
    retried = runner.invoke(cli, ["update", "apply"])
    assert retried.exit_code == 0, retried.output
    assert len(installations) == 2
    assert len(logs) == 1


@pytest.mark.parametrize("state", ["dirty", "ahead", "diverged"])
def test_update_refuses_dirty_or_non_forward_source(runner, update_repositories, state):
    upstream, checkout, git, installations, logs, failure = update_repositories
    (checkout / "source.txt").write_text("Local work must survive\n")
    if state != "dirty":
        git(checkout, "commit", "-am", "Local commit")
    if state == "diverged":
        (upstream / "source.txt").write_text("Different remote work\n")
        git(upstream, "commit", "-am", "Remote divergence")
    before = git(checkout, "rev-parse", "HEAD")
    result = runner.invoke(cli, ["update", "apply"])
    assert result.exit_code != 0
    assert not installations and not logs
    assert git(checkout, "rev-parse", "HEAD") == before
    assert (checkout / "source.txt").read_text() == "Local work must survive\n"


def test_cli_protocol_migration_dry_run_with_sync_disabled(synced_config, runner, monkeypatch):
    from importlib import import_module
    from types import SimpleNamespace
    module = import_module("hyperkb.cli")
    synced_config.set_value("sync_enabled", "false")
    calls = []
    def migrate(*, dry_run):
        calls.append(dry_run)
        return {"status": "dry_run", "upgraded": False,
                "transformation": {"markdown_changed": False, "files_preserved": 3}}
    monkeypatch.setattr(module, "_configured_remote", lambda config: SimpleNamespace(upgrade_protocol=migrate))
    result = runner.invoke(cli, ["sync", "upgrade-protocol", "--dry-run", "--path", synced_config.root])
    assert result.exit_code == 0, result.output
    assert calls == [True]
    assert '"markdown_changed": false' in result.output
    assert '"files_preserved": 3' in result.output
    assert "Migrating the S3 protocol" not in result.output


def test_cli_real_protocol_gate_warns_on_mismatch_and_offline_latch(synced_config, runner, monkeypatch):
    from hyperkb.protocol import ProtocolUnavailable
    class Remote:
        offline = False
        version = 3
        def check_protocol(self):
            if self.offline:
                raise ProtocolUnavailable()
            return {"protocol_version": self.version, "marker_present": True}
    remote = Remote()
    monkeypatch.setattr("hyperkb.remote.S3Remote", lambda *args, **kwargs: remote)
    mismatch = runner.invoke(cli, ["reindex", "--path", synced_config.root])
    assert mismatch.exit_code == 0, mismatch.output
    assert "upgrade_required" in mismatch.output
    assert "Remote sync remains blocked" in mismatch.output
    remote.offline = True
    offline = runner.invoke(cli, ["reindex", "--path", synced_config.root])
    assert offline.exit_code == 0, offline.output
    assert "Local work will continue" in offline.output
    status = runner.invoke(cli, ["sync", "status", "--path", synced_config.root])
    assert status.exit_code != 0
    assert "hkb update apply" in status.output
    remote.offline = False
    remote.version = 2
    restored = runner.invoke(cli, ["reindex", "--path", synced_config.root])
    assert restored.exit_code == 0, restored.output
    assert "Remote sync remains blocked" not in restored.output


def test_cli_protocol_upgrade_reports_actionable_remote_error(synced_config, runner, monkeypatch):
    from importlib import import_module
    from types import SimpleNamespace
    from hyperkb.protocol import ProtocolError
    module = import_module("hyperkb.cli")
    def migrate(*, dry_run):
        raise ProtocolError("upgrade_required", "Run hkb update apply before migrating this newer store.", 3)
    monkeypatch.setattr(module, "_configured_remote", lambda config: SimpleNamespace(upgrade_protocol=migrate))
    result = runner.invoke(cli, ["sync", "upgrade-protocol", "--path", synced_config.root])
    assert result.exit_code != 0
    assert "upgrade_required" in result.output
    assert "hkb update apply" in result.output
    assert "Review deltas" not in result.output
