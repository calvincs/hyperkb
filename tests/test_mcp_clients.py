"""Protocol and lifecycle regressions for independent local MCP clients."""
import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import AsyncExitStack
from pathlib import Path
from unittest.mock import MagicMock

import anyio
import pytest

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from hyperkb.config import KBConfig
from hyperkb.store import KnowledgeStore
from hyperkb import mcp_server as server


def test_two_stdio_clients_share_kb_and_keep_sessions(tmp_path):
    """Opening B preserves A, and both clients can read/write without reconnects."""
    harness = tmp_path / "server.py"
    harness.write_text("import hyperkb.mcp_server as s\ns._check_for_update = lambda: ''\ns.main()\n")

    async def run():
        with anyio.fail_after(30):
            async with AsyncExitStack() as stack:
                async def connect(author):
                    params = StdioServerParameters(
                        command=sys.executable,
                        args=[str(harness), "--path", str(tmp_path / "kb")],
                        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
                             "HKB_SOURCE": author},
                    )
                    streams = await stack.enter_async_context(stdio_client(params))
                    session = await stack.enter_async_context(ClientSession(*streams))
                    await session.initialize()
                    return session

                async def call(session, name, **kwargs):
                    response = await session.call_tool(name, kwargs)
                    assert not response.isError, response
                    return json.loads(response.content[0].text)

                first = await connect("client-a")
                assert len((await first.list_tools()).tools) == 10
                second = await connect("client-b")
                assert len((await first.list_tools()).tools) == 10
                await call(first, "hkb_add", create_file=True, to="shared.notes",
                           description="Shared protocol notes", keywords=["shared"])
                results = []
                async def add(session, content):
                    results.append(await call(session, "hkb_add", to="shared.notes", content=content))
                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(add, first, "shared alpha entry")
                    tasks.start_soon(add, second, "shared beta entry")
                assert all(result["status"] == "ok" for result in results)
                for session in (first, second):
                    rows = await call(session, "hkb_search", query="shared", mode="bm25")
                    assert {row["author"] for row in rows} == {"client-a", "client-b"}
                await call(first, "hkb_session", action="anchor", topics="alpha")
                await call(second, "hkb_session", action="anchor", topics="beta")
                assert (await call(first, "hkb_session", action="anchor"))["anchors"] == ["alpha"]
                assert (await call(second, "hkb_session", action="anchor"))["anchors"] == ["beta"]

    anyio.run(run)


def test_slow_tool_does_not_block_event_loop_and_cancellation_drains(kb_store, monkeypatch):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    ctx = MagicMock()
    ctx.request_context.lifespan_context = server.AppContext(store=kb_store)

    def slow_list(**kwargs):
        started.set()
        assert release.wait(5)
        finished.set()
        return []

    monkeypatch.setattr(kb_store, "list_files", slow_list)

    async def run():
        tool = server.mcp_server._tool_manager.get_tool("hkb_show")
        assert tool.is_async
        async with anyio.create_task_group() as tasks:
            async def invoke():
                await tool.run({}, context=ctx)
            tasks.start_soon(invoke)
            while not started.is_set():
                await anyio.sleep(0.005)
            # A cancellation is delivered while the mutation thread is held;
            # event-loop callbacks continue and cleanup drains the operation.
            tasks.cancel_scope.cancel()
            assert not finished.is_set()
            release.set()
        assert finished.is_set()

    anyio.run(run)


def test_startup_recovers_missing_index_without_changing_config(tmp_path, monkeypatch):
    cfg = KBConfig(root=str(tmp_path), default_source="configured-author", rg_weight=0.7)
    store = KnowledgeStore(cfg)
    store.init()
    store.create_file("recovery.notes", "Recovery notes", ["recovery"])
    store.add_entry("surviving markdown entry", file_name="recovery.notes")
    store.close()
    previous = cfg.config_path.read_bytes()
    cfg.db_path.unlink()
    monkeypatch.setattr(server, "_server_args", server._parse_server_args(["--path", str(tmp_path)]))
    monkeypatch.setattr(server, "_check_for_update", lambda: "")

    async def run():
        async with server.app_lifespan(server.mcp_server) as ctx:
            assert len(ctx.store.search("surviving", mode="bm25")) == 1
            assert ctx.store.config.default_source == "configured-author"
    anyio.run(run)
    assert cfg.config_path.read_bytes() == previous


@pytest.mark.parametrize("name,param", [
    ("hkb_search", "mode"), ("hkb_context", "mode"), ("hkb_view", "action"),
    ("hkb_task", "action"), ("hkb_session", "action"), ("hkb_health", "action"),
    ("hkb_update", "action"), ("hkb_sync", "action"),
])
def test_tool_action_schema_rejects_unknown(name, param):
    tool = server.mcp_server._tool_manager.get_tool(name)
    assert "enum" in tool.parameters["properties"][param]
    arguments = {param: "typo"}
    if name == "hkb_context":
        arguments["topic"] = "anything"
    with pytest.raises(ValueError):
        tool.fn_metadata.arg_model.model_validate(arguments)


def test_supported_dependency_and_import():
    # Both install extras must constrain the API major version we exercise.
    from packaging.requirements import Requirement
    project = Path(__file__).resolve().parents[1] / "pyproject.toml"
    declarations = [line.strip().strip(',').strip('"') for line in project.read_text().splitlines()
                    if '"mcp[cli]' in line]
    assert len(declarations) == 2
    for declaration in declarations:
        requirement = Requirement(declaration.split('["')[-1].rstrip('"]'))
        assert "1.30.0" in requirement.specifier
        assert "2.0.0" not in requirement.specifier
    completed = subprocess.run([sys.executable, "-c", "import hyperkb.mcp_server"],
                               capture_output=True, text=True, timeout=15)
    assert completed.returncode == 0, completed.stderr


def test_sigterm_drains_active_work_before_store_close(tmp_path):
    """The normal SIGTERM path completes active work and executes close()."""
    harness = tmp_path / "shutdown_server.py"
    marker = tmp_path / "lifecycle.txt"
    harness.write_text('''import time
from pathlib import Path
import hyperkb.mcp_server as server
marker = Path(__import__('sys').argv[1])
__import__('sys').argv.pop(1)
server._check_for_update = lambda: ''
original_close = server.KnowledgeStore.close
original_list = server.KnowledgeStore.list_files
def slow_list(self, *args, **kwargs):
    marker.write_text('started\\n')
    time.sleep(0.4)
    with marker.open('a') as stream:
        stream.write('finished\\n')
    return original_list(self, *args, **kwargs)
def close(self):
    with marker.open('a') as stream:
        stream.write('closed\\n')
    original_close(self)
server.KnowledgeStore.list_files = slow_list
server.KnowledgeStore.close = close
server.main()
''')
    proc = subprocess.Popen(
        [sys.executable, str(harness), str(marker), "--path", str(tmp_path / "kb")],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )
    try:
        import selectors
        def send(message):
            proc.stdin.write(json.dumps(message) + "\n")
            proc.stdin.flush()
        send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "shutdown-test", "version": "1"}}})
        with selectors.DefaultSelector() as selector:
            selector.register(proc.stdout, selectors.EVENT_READ)
            assert selector.select(15), "Server failed to initialize"
        assert json.loads(proc.stdout.readline())["id"] == 1
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        send({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
              "params": {"name": "hkb_show", "arguments": {}}})
        deadline = time.monotonic() + 10
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists(), "Slow operation did not start"
        proc.send_signal(signal.SIGTERM)
        proc.communicate(timeout=15)
        assert proc.returncode == 0
        assert marker.read_text().splitlines() == ["started", "finished", "closed"]
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate(timeout=5)


def test_batch_truncation_is_explicit(kb_store):
    ctx = MagicMock()
    ctx.request_context.lifespan_context = server.AppContext(store=kb_store)
    items = [{"file": "missing.file", "epoch": i, "set_status": "active"} for i in range(51)]
    result = json.loads(server.hkb_update(action="batch", updates=json.dumps(items), ctx=ctx))
    assert result["requested"] == 51
    assert result["processed"] == 50
    assert result["skipped"] == 1
    assert result["truncated"] is True


def test_task_pagination_reports_total_and_next_offset(kb_store):
    ctx = MagicMock()
    ctx.request_context.lifespan_context = server.AppContext(store=kb_store)
    for index in range(3):
        result = json.loads(server.hkb_task(action="create", title=f"Task {index}", ctx=ctx))
        assert result["status"] == "ok"
    first = json.loads(server.hkb_task(top=2, ctx=ctx))
    second = json.loads(server.hkb_task(top=2, offset=first["next_offset"], ctx=ctx))
    assert first["total"] == second["total"] == 3
    assert first["has_more"] is True
    assert second["has_more"] is False
    assert second["next_offset"] is None
    assert len(first["tasks"]) == 2 and len(second["tasks"]) == 1
    assert {task["epoch"] for task in first["tasks"]}.isdisjoint(task["epoch"] for task in second["tasks"])
    assert first["_summary"]["pending"] == 3


def test_show_returns_entry_metadata(kb_store, monkeypatch):
    monkeypatch.setenv("HKB_SOURCE", "show-client")
    kb_store.create_file("show.notes", "Metadata visibility", ["metadata"])
    kb_store.add_entry("@weight: high\n@type: decision\nKeep this decision", file_name="show.notes")
    ctx = MagicMock()
    ctx.request_context.lifespan_context = server.AppContext(store=kb_store)
    entry = json.loads(server.hkb_show(name="show.notes", ctx=ctx))["entries"][0]
    assert entry["metadata"]["author"] == "show-client"
    assert entry["metadata"]["weight"] == "high"
    assert entry["metadata"]["type"] == "decision"


def test_context_preserves_serialized_budget_with_session_anchors(kb_store):
    kb_store.create_file("context.notes", "Context budget " * 20, ["context"])
    kb_store.add_entry("Context details " * 10, file_name="context.notes")
    ctx = MagicMock()
    ctx.request_context.lifespan_context = server.AppContext(
        store=kb_store, anchors=["context"], anchor_files={"context.notes": 100},
    )
    response = server.hkb_context(topic="context", max_tokens=200, ctx=ctx)
    result = json.loads(response)
    assert kb_store._estimate_tokens(response) <= 200
    assert result["tokens_used"] == kb_store._estimate_tokens(response)
