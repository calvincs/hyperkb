"""Tests for MCP sync tools — consolidated hkb_sync(action=...) interface."""

import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

mcp = pytest.importorskip("mcp")

from hyperkb.config import KBConfig
from hyperkb.store import KnowledgeStore
from hyperkb.mcp_server import (
    AppContext,
    hkb_sync,
    hkb_health,
    _parse_duration_seconds,
)


def _make_ctx(store, health=None, sync_worker=None):
    """Build a mock Context with store and optional sync_worker."""
    ctx = MagicMock()
    ctx.request_context.lifespan_context = AppContext(
        store=store, health=health, sync_worker=sync_worker,
    )
    return ctx


# ---------------------------------------------------------------------------
# hkb_sync
# ---------------------------------------------------------------------------

class TestHkbSync:
    def test_sync_disabled(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(ctx=ctx))
        assert result["status"] == "error"
        assert "not enabled" in result["message"]

    def test_sync_enabled_no_engine(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="",  # No bucket = can't create engine
        )
        store = KnowledgeStore(config)
        store.init()
        ctx = _make_ctx(store)
        result = json.loads(hkb_sync(ctx=ctx))
        assert result["status"] == "error"
        assert "Could not initialize" in result["message"]
        store.close()

    def test_sync_invalid_direction(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="test-bucket",
        )
        store = KnowledgeStore(config)
        store.init()
        ctx = _make_ctx(store)

        # Mock the sync engine via worker
        mock_engine = MagicMock()
        mock_worker = MagicMock()
        mock_worker.engine = mock_engine
        ctx.request_context.lifespan_context.sync_worker = mock_worker

        result = json.loads(hkb_sync(action="invalid", ctx=ctx))
        assert result["status"] == "error"
        assert "action must be" in result["message"]
        store.close()

    def test_sync_via_worker_engine(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="test-bucket",
        )
        store = KnowledgeStore(config)
        store.init()

        mock_engine = MagicMock()
        mock_engine.sync.return_value = {
            "status": "ok",
            "pushed": [],
            "pulled": [],
            "conflicts": [],
        }
        mock_worker = MagicMock()
        mock_worker.engine = mock_engine

        ctx = _make_ctx(store, sync_worker=mock_worker)
        result = json.loads(hkb_sync(action="both", ctx=ctx))
        assert result["status"] == "ok"
        mock_engine.sync.assert_called_once_with(direction="both", dry_run=False)
        store.close()


# ---------------------------------------------------------------------------
# hkb_sync_status
# ---------------------------------------------------------------------------

class TestHkbSyncStatus:
    def test_status_sync_disabled(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="status", ctx=ctx))
        assert result["sync_enabled"] is False
        assert result["worker_running"] is False

    def test_status_with_worker(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="test-bucket",
            sync_interval=300,
        )
        store = KnowledgeStore(config)
        store.init()

        mock_engine = MagicMock()
        mock_engine.get_status.return_value = {
            "sync_enabled": True,
            "git_initialized": True,
            "last_sync_time": 1000000,
            "last_sync_status": "ok",
            "last_sync_error": "",
            "local_pending_files": [],
            "local_pending_count": 0,
            "commits_since_sync": 2,
            "bucket": "test-bucket",
            "prefix": "hkb/",
        }
        mock_worker = MagicMock()
        mock_worker.engine = mock_engine
        mock_worker.is_running = True

        ctx = _make_ctx(store, sync_worker=mock_worker)
        result = json.loads(hkb_sync(action="status", ctx=ctx))
        assert result["sync_enabled"] is True
        assert result["worker_running"] is True
        assert result["last_sync_status"] == "ok"
        store.close()


# ---------------------------------------------------------------------------
# hkb_sync_conflicts
# ---------------------------------------------------------------------------

class TestHkbSyncConflicts:
    def test_conflicts_sync_disabled(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="conflicts", ctx=ctx))
        assert result["status"] == "error"

    def test_conflicts_list_empty(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="test-bucket",
        )
        store = KnowledgeStore(config)
        store.init()

        mock_engine = MagicMock()
        mock_engine.get_conflict_log.return_value = []
        mock_worker = MagicMock()
        mock_worker.engine = mock_engine

        ctx = _make_ctx(store, sync_worker=mock_worker)
        result = json.loads(hkb_sync(action="conflicts", conflict_action="list", ctx=ctx))
        assert result["count"] == 0
        assert result["conflicts"] == []
        store.close()

    def test_conflicts_clear(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="test-bucket",
        )
        store = KnowledgeStore(config)
        store.init()

        mock_engine = MagicMock()
        mock_engine.clear_conflict_log.return_value = 3
        mock_worker = MagicMock()
        mock_worker.engine = mock_engine

        ctx = _make_ctx(store, sync_worker=mock_worker)
        result = json.loads(hkb_sync(action="conflicts", conflict_action="clear", ctx=ctx))
        assert result["status"] == "ok"
        assert result["cleared"] == 3
        store.close()

    def test_conflicts_invalid_action(self, tmp_path):
        config = KBConfig(
            root=str(tmp_path),
            sync_enabled=True,
            sync_bucket="test-bucket",
        )
        store = KnowledgeStore(config)
        store.init()

        mock_engine = MagicMock()
        mock_worker = MagicMock()
        mock_worker.engine = mock_engine

        ctx = _make_ctx(store, sync_worker=mock_worker)
        result = json.loads(hkb_sync(action="conflicts", conflict_action="invalid", ctx=ctx))
        assert result["status"] == "error"
        store.close()


# ---------------------------------------------------------------------------
# hkb_sync_config
# ---------------------------------------------------------------------------

class TestHkbSyncConfig:
    def test_view_all_config(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="config", ctx=ctx))
        assert "sync_enabled" in result
        assert "sync_bucket" in result
        assert "sync_interval" in result

    def test_set_bucket(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="config", key="sync_bucket", value="my-bucket", ctx=ctx))
        assert result["status"] == "ok"
        assert result["value"] == "my-bucket"
        # Verify it persisted
        assert kb_store.config.sync_bucket == "my-bucket"

    def test_set_enabled(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="config", key="sync_enabled", value="true", ctx=ctx))
        assert result["status"] == "ok"
        assert kb_store.config.sync_enabled is True

    def test_set_interval(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="config", key="sync_interval", value="60", ctx=ctx))
        assert result["status"] == "ok"
        assert kb_store.config.sync_interval == 60

    def test_reject_non_sync_key(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="config", key="rg_weight", value="test", ctx=ctx))
        assert result["status"] == "error"
        assert "sync_" in result["message"]

    def test_reject_unknown_key(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_sync(action="config", key="sync_nonexistent", value="x", ctx=ctx))
        assert result["status"] == "error"
        assert "Unknown" in result["message"]

    def test_sensitive_fields_masked(self, kb_store):
        ctx = _make_ctx(kb_store)
        # Set a key then view
        hkb_sync(action="config", key="sync_access_key", value="AKIAIOSFODNN7EXAMPLE", ctx=ctx)
        result = json.loads(hkb_sync(action="config", ctx=ctx))
        # Access key should be masked
        assert result["sync_access_key"] != "AKIAIOSFODNN7EXAMPLE"
        assert "****" in result["sync_access_key"]


# ---------------------------------------------------------------------------
# Compaction sync warning
# ---------------------------------------------------------------------------

class TestCompactSyncWarning:
    def test_compact_warns_when_sync_enabled(self, sample_kb):
        # Enable sync on the store
        sample_kb.config.sync_enabled = True
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(action="compact",
            file="security.threat-intel",
            dry_run=False,
            ctx=ctx,
        ))
        # Should have sync warning (even if compaction did nothing)
        if result.get("status") != "error":
            assert "sync_warning" in result

    def test_compact_no_warning_when_sync_disabled(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(action="compact",
            file="security.threat-intel",
            dry_run=False,
            ctx=ctx,
        ))
        assert "sync_warning" not in result

    def test_compact_no_warning_on_dry_run(self, sample_kb):
        sample_kb.config.sync_enabled = True
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(action="compact",
            file="security.threat-intel",
            dry_run=True,
            ctx=ctx,
        ))
        assert "sync_warning" not in result
