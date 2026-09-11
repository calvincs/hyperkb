"""Registered MCP tools keep local work available behind a remote protocol fence."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import anyio
import pytest

pytest.importorskip("mcp")
from mcp.server.fastmcp.exceptions import ToolError

from hyperkb import mcp_server as server
from hyperkb.protocol import ProtocolGate, ProtocolUnavailable
from hyperkb.store import KnowledgeStore


class MutableRemote:
    def __init__(self, version=2):
        self.version = version
        self.failure = None
        self.checks = 0

    def check_protocol(self):
        self.checks += 1
        if self.failure:
            raise self.failure
        return {"protocol_version": self.version}


def make_context(store, remote):
    engine = MagicMock()
    engine.get_status.return_value = {"pending_uploads": True}
    engine.sync.return_value = {"status": "ok", "pushed": ["protocol.notes.md"]}
    engine.get_conflict_log.return_value = []
    worker = SimpleNamespace(engine=engine, is_running=False, is_leader=False)
    app = server.AppContext(store=store, protocol_gate=ProtocolGate(store.config, remote), sync_worker=worker)
    return SimpleNamespace(request_context=SimpleNamespace(lifespan_context=app), warning=AsyncMock())


def invoke(ctx, tool_name, **arguments):
    """Use FastMCP's registered adapter, including validation and protocol checks."""
    async def run():
        tool = server.mcp_server._tool_manager.get_tool(tool_name)
        assert tool.is_async
        with anyio.fail_after(5):
            return await tool.run(arguments, context=ctx)
    return json.loads(anyio.run(run))


@pytest.fixture
def protocol_client(kb_store, monkeypatch):
    kb_store.create_file("protocol.notes", "Protocol continuity notes", ["continuity"])
    kb_store.add_entry("original continuity entry", "protocol.notes", epoch=1000)
    cfg = kb_store.config
    cfg.sync_enabled = True
    cfg.sync_bucket = "private-test-bucket"
    cfg.sync_access_key = "ACCESS_KEY_DO_NOT_EXPOSE"
    cfg.sync_secret_key = "SECRET_KEY_DO_NOT_EXPOSE"
    cfg.sync_endpoint_url = "https://endpoint.invalid/private"
    # These tests exercise the MCP fence and actual markdown/index persistence;
    # the sync suites cover Git auto-commit and eventual upload reconciliation.
    monkeypatch.setattr(kb_store, "_sync_commit", lambda *args: None)
    remote = MutableRemote()
    return kb_store, remote, make_context(kb_store, remote)


@pytest.mark.parametrize("name,arguments", [
    ("hkb_search", {"query": "continuity", "mode": "bm25"}),
    ("hkb_show", {"name": "protocol.notes"}),
    ("hkb_add", {"content": "new continuity entry", "to": "protocol.notes"}),
    ("hkb_update", {"file": "protocol.notes", "epoch": 1000, "new_content": "updated continuity entry"}),
    ("hkb_session", {"action": "anchor", "topics": "continuity"}),
    ("hkb_health", {"action": "check", "checks": "quick"}),
    ("hkb_task", {"action": "list"}),
    ("hkb_sync", {"action": "status"}),
    ("hkb_context", {"topic": "continuity", "max_tokens": 1000}),
    ("hkb_view", {"action": "list"}),
])
def test_every_registered_tool_rechecks_and_warns_without_stopping_local_work(protocol_client, name, arguments):
    _, remote, ctx = protocol_client
    first = invoke(ctx, name, **arguments)
    assert not isinstance(first, dict) or "_protocol" not in first
    assert not isinstance(first, dict) or first.get("status") != "error", first
    checks_before = remote.checks
    remote.version = 3
    result = invoke(ctx, name, **arguments)
    assert result.get("status") != "error", result
    assert remote.checks == checks_before + 1
    warning = result["_protocol"]
    assert warning["status"] == "upgrade_required"
    assert warning["local_operations_allowed"] is True
    assert warning["remote_sync_blocked"] is True
    assert warning["remote_protocol_version"] == 3
    assert "hkb update apply" in warning["message"]
    assert "reconcile" in warning["message"]
    ctx.warning.assert_awaited_once()


@pytest.mark.parametrize("action", ["push", "pull", "both"])
def test_remote_sync_is_blocked_before_engine_resolution(protocol_client, monkeypatch, action):
    _, remote, ctx = protocol_client
    invoke(ctx, "hkb_show", name="protocol.notes")
    remote.version = 3
    resolve_engine = MagicMock(side_effect=AssertionError("Must fence before resolving the sync engine"))
    monkeypatch.setattr(server, "_get_sync_engine", resolve_engine)
    with pytest.raises(ToolError) as error:
        invoke(ctx, "hkb_sync", action=action)
    assert "upgrade_required" in str(error.value)
    assert "keep recording locally" in str(error.value)
    resolve_engine.assert_not_called()
    ctx.request_context.lifespan_context.sync_worker.engine.sync.assert_not_called()


def test_local_adds_survive_restart_and_offline_latch_then_verified_protocol_clears(protocol_client, monkeypatch):
    store, remote, first_ctx = protocol_client
    invoke(first_ctx, "hkb_search", query="continuity", mode="bm25")
    remote.version = 3
    added = invoke(first_ctx, "hkb_add", content="continuity recorded during upgrade", to="protocol.notes")
    assert added["status"] == "ok"
    assert added["_protocol"]["status"] == "upgrade_required"
    store.close()

    restarted = KnowledgeStore(store.config)
    restarted.recover_index()
    monkeypatch.setattr(restarted, "_sync_commit", lambda *args: None)
    remote.failure = ProtocolUnavailable()
    second_ctx = make_context(restarted, remote)
    try:
        found = invoke(second_ctx, "hkb_search", query="upgrade", mode="bm25")
        assert found["_protocol"]["status"] == "upgrade_required"
        assert "recorded during upgrade" in found["result"][0]["content"]
        offline_add = invoke(second_ctx, "hkb_add", content="continuity recorded while offline", to="protocol.notes")
        assert offline_add["status"] == "ok"
        assert offline_add["_protocol"]["remote_protocol_version"] == 3
        with pytest.raises(ToolError):
            invoke(second_ctx, "hkb_sync", action="both")
        remote.failure = None
        remote.version = 2
        restored = invoke(second_ctx, "hkb_search", query="recorded", mode="bm25")
        assert isinstance(restored, list)
        assert len(restored) == 2
        assert not list(store.config.hkb_dir.glob("sync/protocol-*.json"))
        synced = invoke(second_ctx, "hkb_sync", action="both", dry_run=True)
        assert synced["status"] == "ok"
        assert "_protocol" not in synced
        second_ctx.request_context.lifespan_context.sync_worker.engine.sync.assert_called_once_with(direction="both", dry_run=True)
    finally:
        restarted.close()


def test_offline_without_prior_mismatch_warns_and_keeps_recording(protocol_client):
    _, remote, ctx = protocol_client
    remote.failure = ProtocolUnavailable()
    result = invoke(ctx, "hkb_add", content="continuity offline first contact", to="protocol.notes")
    assert result["status"] == "ok"
    assert result["_protocol"]["status"] == "offline"
    assert result["_protocol"]["local_operations_allowed"] is True
    with pytest.raises(ToolError):
        invoke(ctx, "hkb_sync", action="push")


def test_provider_failure_does_not_expose_credentials_in_tools_logs_or_saved_state(protocol_client, caplog):
    store, remote, ctx = protocol_client
    secret = store.config.sync_secret_key
    access = store.config.sync_access_key
    endpoint = store.config.sync_endpoint_url
    remote.failure = RuntimeError(f"provider credentials {access}:{secret} at {endpoint}")
    local = invoke(ctx, "hkb_add", content="continuity during provider error", to="protocol.notes")
    assert local["status"] == "ok"
    assert local["_protocol"]["status"] == "protocol_unverified"
    with pytest.raises(ToolError) as error:
        invoke(ctx, "hkb_sync", action="pull")
    config = invoke(ctx, "hkb_sync", action="config")
    assert config.get("status") != "error", config
    assert config["sync_access_key"] != access
    assert config["sync_secret_key"] != secret
    # Explicit config inspection can show endpoint settings; protocol notices,
    # provider errors, warning logs and the persisted latch must not reveal them.
    latch = "".join(path.read_text() for path in store.config.hkb_dir.glob("sync/protocol-*.json"))
    public = json.dumps(local) + str(error.value) + caplog.text + latch
    assert secret not in public
    assert access not in public
    assert endpoint not in public
    assert secret not in json.dumps(config)
    assert access not in json.dumps(config)


@pytest.mark.parametrize("budget", [100, 1000])
def test_context_budget_includes_protocol_notice_and_reports_mandatory_overhead(protocol_client, budget):
    store, remote, ctx = protocol_client
    remote.version = 3
    result = invoke(ctx, "hkb_context", topic="continuity", max_tokens=budget)
    assert result.get("status") != "error", result
    assert result["_protocol"]["status"] == "upgrade_required"
    assert result["tokens_used"] == store._estimate_tokens(json.dumps(result, indent=2))
    assert result["tokens_budget"] == budget
    if budget == 100:
        assert result["budget_exceeded"] is True
        assert result["entries"] == []
        assert result["diagnostic_overhead_tokens"] > 0
        assert result["tokens_used"] > budget
    else:
        assert not result.get("budget_exceeded", False)
        assert any(entry["content"] == "original continuity entry" for entry in result["entries"])
        assert result["tokens_used"] <= budget


def test_store_context_reserves_diagnostics_before_packing_and_snapshots_them(protocol_client):
    store, _, _ = protocol_client
    metadata = {"_protocol": {"status": "upgrade_required", "message": "x" * 1000}}
    result = store.build_context("continuity", max_tokens=350, response_metadata=metadata)
    assert result["tokens_used"] == store._estimate_tokens(json.dumps(result, indent=2))
    assert result["tokens_used"] <= 350
    assert result["entries"] == []
    metadata["_protocol"]["message"] = "changed after packing"
    assert result["_protocol"]["message"] == "x" * 1000


def test_store_context_metadata_cannot_override_budget_or_entries(protocol_client):
    store, _, _ = protocol_client
    with pytest.raises(ValueError, match="internal _protocol"):
        store.build_context("continuity", response_metadata={"tokens_used": 0})


def test_context_wrapper_preserves_counted_protocol_snapshot(protocol_client, monkeypatch):
    store, remote, ctx = protocol_client
    remote.version = 3
    original_pack = store.build_context
    def pack_then_refresh(*args, **kwargs):
        result = original_pack(*args, **kwargs)
        # Simulate another request observing a new version after packing but
        # before the registered adapter attaches protocol diagnostics.
        remote.version = 4
        ctx.request_context.lifespan_context.protocol_gate.check(allow_local=True)
        return result
    monkeypatch.setattr(store, "build_context", pack_then_refresh)
    result = invoke(ctx, "hkb_context", topic="continuity", max_tokens=1000)
    assert result["_protocol"]["remote_protocol_version"] == 3
    assert result["tokens_used"] == store._estimate_tokens(json.dumps(result, indent=2))
    assert result["tokens_used"] <= 1000
