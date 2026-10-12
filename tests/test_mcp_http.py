"""Streamable HTTP transport for clients that cannot launch a local process."""
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path

import anyio
import httpx
import pytest

pytest.importorskip("mcp")
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from hyperkb import mcp_server as server


def test_non_loopback_bind_requires_a_token():
    with pytest.raises(SystemExit):
        server._http_app("0.0.0.0", "", [])


def test_http_clients_need_the_token_and_keep_separate_room_identities(tmp_path):
    harness = tmp_path / "server.py"
    harness.write_text("import hyperkb.mcp_server as s\ns._check_for_update = lambda: ''\ns.main()\n")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    url = f"http://127.0.0.1:{port}/mcp"
    proc = subprocess.Popen(
        [sys.executable, str(harness), "--http", "--port", str(port), "--path", str(tmp_path / "kb")],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
             "HKB_HTTP_TOKEN": "s3cret", "HKB_SOURCE": "http-clients"},
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            try:
                assert httpx.post(url, json={}, timeout=2).status_code == 401
                break
            except httpx.TransportError:
                assert proc.poll() is None and time.monotonic() < deadline, "HTTP server did not start"
                time.sleep(0.1)
        assert httpx.post(url, json={}, headers={"Authorization": "Bearer wrong"}, timeout=5).status_code == 401

        async def run():
            with anyio.fail_after(30):
                async with AsyncExitStack() as stack:
                    async def connect():
                        read, write, _ = await stack.enter_async_context(
                            streamablehttp_client(url, headers={"Authorization": "Bearer s3cret"}))
                        session = await stack.enter_async_context(ClientSession(read, write))
                        await session.initialize()
                        return session

                    async def call(session, tool, /, **kwargs):
                        response = await session.call_tool(tool, kwargs)
                        assert not response.isError, response
                        return json.loads(response.content[0].text)

                    first, second = await connect(), await connect()
                    assert len((await first.list_tools()).tools) == 11
                    await call(first, "hkb_room", action="join", room="atlas", name="pernix")
                    await call(second, "hkb_room", action="join", room="atlas", name="codex-1")
                    await call(first, "hkb_room", action="send", to="codex-1", message="over http")
                    inbox = await call(second, "hkb_room", action="checkin")
                    assert inbox["you"] == "codex-1"
                    assert [m["from"] for m in inbox["unread"]] == ["pernix"]
                    assert (await call(first, "hkb_room", action="checkin"))["you"] == "pernix"

        anyio.run(run)
    finally:
        proc.terminate()
        proc.communicate(timeout=15)
