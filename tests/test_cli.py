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
