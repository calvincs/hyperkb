"""Tests for entry provenance tracking (@author, @hostname).

Tests cover: config, db migration/insert/search/recent, store auto-populate
and threading, search enrichment, and MCP tool output/filters.
"""

import json
import os
import socket
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from hyperkb.config import KBConfig, _ENV_OVERRIDES
from hyperkb.db import KBDatabase
from hyperkb.models import SearchResult
from hyperkb.store import KnowledgeStore

mcp = pytest.importorskip("mcp")

from hyperkb.mcp_server import (
    AppContext,
    hkb_search,
    hkb_session,
)


def _make_ctx(store, health=None):
    ctx = MagicMock()
    ctx.request_context.lifespan_context = AppContext(store=store, health=health)
    return ctx


# ---------------------------------------------------------------------------
# config.py
# ---------------------------------------------------------------------------

class TestConfigProvenance:
    def test_default_source_field_exists(self):
        cfg = KBConfig()
        assert cfg.default_source == ""

    def test_hkb_source_env_override(self, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "test-client")
        cfg = KBConfig._from_dict({"root": "/tmp/test"})
        assert cfg.default_source == "test-client"

    def test_hkb_source_in_env_overrides(self):
        assert "default_source" in _ENV_OVERRIDES
        assert _ENV_OVERRIDES["default_source"] == "HKB_SOURCE"


# ---------------------------------------------------------------------------
# db.py
# ---------------------------------------------------------------------------

class TestDbProvenance:
    def test_migration_adds_columns(self, kb_db):
        conn = kb_db.connect()
        cols = {row[1] for row in conn.execute("PRAGMA table_info(entries)").fetchall()}
        assert "author" in cols
        assert "hostname" in cols

    def test_insert_entry_with_provenance(self, kb_db):
        kb_db.insert_file(
            __import__("hyperkb.models", fromlist=["FileHeader"]).FileHeader(
                name="test.file", description="test", keywords=[], links=[], created="now"
            ),
            path="test.file.md",
        )
        _, epoch = kb_db.insert_entry(
            "test.file", 1000, "content", author="claude-code", hostname="machine1"
        )
        row = kb_db.get_entry("test.file", epoch)
        assert row["author"] == "claude-code"
        assert row["hostname"] == "machine1"

    def test_bm25_filter_by_author(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="prov.test", description="provenance test", keywords=[], links=[], created="now"),
            path="prov.test.md",
        )
        kb_db.insert_entry("prov.test", 1001, "finding about provenance", author="claude-code", hostname="h1")
        kb_db.insert_entry("prov.test", 1002, "finding about provenance too", author="opencode", hostname="h2")

        results = kb_db.bm25_search_entries("provenance", author="claude-code")
        assert len(results) == 1
        assert results[0].author == "claude-code"

    def test_bm25_filter_by_hostname(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="host.test", description="hostname test", keywords=[], links=[], created="now"),
            path="host.test.md",
        )
        kb_db.insert_entry("host.test", 2001, "entry about hostname filtering", author="a", hostname="laptop")
        kb_db.insert_entry("host.test", 2002, "entry about hostname filtering too", author="b", hostname="server")

        results = kb_db.bm25_search_entries("hostname", hostname="laptop")
        assert len(results) == 1
        assert results[0].hostname == "laptop"

    def test_bm25_returns_provenance_fields(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="ret.test", description="return test", keywords=[], links=[], created="now"),
            path="ret.test.md",
        )
        kb_db.insert_entry("ret.test", 3001, "provenance return test", author="hermes", hostname="desktop")
        results = kb_db.bm25_search_entries("provenance")
        assert len(results) >= 1
        r = results[0]
        assert r.author == "hermes"
        assert r.hostname == "desktop"

    def test_get_recent_filter_by_author(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="rec.test", description="recent test", keywords=[], links=[], created="now"),
            path="rec.test.md",
        )
        kb_db.insert_entry("rec.test", 4001, "entry one", author="claude-code", hostname="h1")
        kb_db.insert_entry("rec.test", 4002, "entry two", author="opencode", hostname="h2")

        entries = kb_db.get_recent_entries(author="opencode")
        assert len(entries) == 1
        assert entries[0]["author"] == "opencode"

    def test_get_recent_filter_by_hostname(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="rech.test", description="recent host test", keywords=[], links=[], created="now"),
            path="rech.test.md",
        )
        kb_db.insert_entry("rech.test", 5001, "entry alpha", author="a", hostname="workstation")
        kb_db.insert_entry("rech.test", 5002, "entry beta", author="b", hostname="server")

        entries = kb_db.get_recent_entries(hostname="workstation")
        assert len(entries) == 1
        assert entries[0]["hostname"] == "workstation"

    def test_update_entry_with_provenance(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="upd.test", description="update test", keywords=[], links=[], created="now"),
            path="upd.test.md",
        )
        kb_db.insert_entry("upd.test", 6001, "original", author="old-author", hostname="old-host")
        kb_db.update_entry("upd.test", 6001, author="new-author", hostname="new-host")
        row = kb_db.get_entry("upd.test", 6001)
        assert row["author"] == "new-author"
        assert row["hostname"] == "new-host"

    def test_get_tasks_includes_provenance(self, kb_db):
        from hyperkb.models import FileHeader
        kb_db.insert_file(
            FileHeader(name="task.test", description="task test", keywords=[], links=[], created="now"),
            path="task.test.md",
        )
        kb_db.insert_entry(
            "task.test", 7001, "do something",
            entry_type="task", status="pending",
            author="claude-code", hostname="dev-machine",
        )
        tasks = kb_db.get_tasks(status_filter="pending")
        assert len(tasks) >= 1
        t = [t for t in tasks if t["epoch"] == 7001][0]
        assert t["author"] == "claude-code"
        assert t["hostname"] == "dev-machine"


# ---------------------------------------------------------------------------
# store.py
# ---------------------------------------------------------------------------

class TestStoreProvenance:
    def test_get_author_env_var(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "env-client")
        assert kb_store._get_author() == "env-client"

    def test_get_author_config_fallback(self, kb_store, monkeypatch):
        monkeypatch.delenv("HKB_SOURCE", raising=False)
        kb_store.config.default_source = "config-client"
        assert kb_store._get_author() == "config-client"

    def test_get_author_unknown_fallback(self, kb_store, monkeypatch):
        monkeypatch.delenv("HKB_SOURCE", raising=False)
        kb_store.config.default_source = ""
        assert kb_store._get_author() == "unknown"

    def test_append_entry_auto_populates_author(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "test-ai")
        kb_store.create_file("prov.auto", "auto provenance test", ["test"])
        kb_store.add_entry(content="auto author test", file_name="prov.auto", epoch=100000)
        row = kb_store.db.get_entry("prov.auto", 100000)
        assert row["author"] == "test-ai"

    def test_append_entry_auto_populates_hostname(self, kb_store):
        kb_store.create_file("prov.host", "hostname test", ["test"])
        kb_store.add_entry(content="auto hostname test", file_name="prov.host", epoch=200000)
        row = kb_store.db.get_entry("prov.host", 200000)
        assert row["hostname"] == socket.gethostname()

    def test_explicit_author_in_content_preserved(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "default-ai")
        kb_store.create_file("prov.explicit", "explicit author test", ["test"])
        kb_store.add_entry(
            content="@author: custom-author\nSome content here",
            file_name="prov.explicit",
            epoch=300000,
        )
        row = kb_store.db.get_entry("prov.explicit", 300000)
        assert row["author"] == "custom-author"

    def test_search_threads_author_filter(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "search-test")
        kb_store.create_file("prov.search", "search provenance", ["provenance"])
        kb_store.add_entry(content="provenance search entry", file_name="prov.search", epoch=400000)
        results = kb_store.search("provenance", author="search-test")
        assert len(results) >= 1
        assert all(r.author == "search-test" for r in results if r.source == "bm25")

    def test_get_recent_threads_author_filter(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "recent-test")
        kb_store.create_file("prov.recent", "recent provenance", ["test"])
        kb_store.add_entry(content="recent entry one", file_name="prov.recent", epoch=500000)
        monkeypatch.setenv("HKB_SOURCE", "other-client")
        kb_store.add_entry(content="recent entry two", file_name="prov.recent", epoch=500001)

        entries = kb_store.get_recent(author="recent-test")
        assert len(entries) == 1
        assert entries[0]["author"] == "recent-test"

    def test_reindex_preserves_provenance(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "reindex-test")
        kb_store.create_file("prov.reindex", "reindex test", ["test"])
        kb_store.add_entry(content="@author: original-ai\n@hostname: original-host\nreindex content", file_name="prov.reindex", epoch=600000)
        kb_store.reindex()
        row = kb_store.db.get_entry("prov.reindex", 600000)
        assert row["author"] == "original-ai"
        assert row["hostname"] == "original-host"

    def test_reindex_includes_weight(self, kb_store):
        """Bug fix: reindex was missing weight column."""
        kb_store.create_file("prov.weight", "weight test", ["test"])
        kb_store.add_entry(content="@weight: high\nimportant finding", file_name="prov.weight", epoch=700000)
        kb_store.reindex()
        row = kb_store.db.get_entry("prov.weight", 700000)
        assert row["weight"] == "high"


# ---------------------------------------------------------------------------
# search.py
# ---------------------------------------------------------------------------

class TestSearchProvenance:
    def test_enrich_metadata_fills_author_hostname(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "enrich-test")
        kb_store.create_file("prov.enrich", "enrich test", ["enrich"])
        kb_store.add_entry(content="enrichment test content", file_name="prov.enrich", epoch=800000)

        # Create an rg-like result without metadata
        r = SearchResult(file_name="prov.enrich", content="enrichment test", epoch=800000, source="rg")
        kb_store.search_engine._enrich_metadata([r])
        assert r.author == "enrich-test"
        assert r.hostname == socket.gethostname()

    def test_search_passes_author_filter(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "filter-a")
        kb_store.create_file("prov.filter", "filter test", ["filter"])
        kb_store.add_entry(content="filter test alpha", file_name="prov.filter", epoch=900000)
        monkeypatch.setenv("HKB_SOURCE", "filter-b")
        kb_store.add_entry(content="filter test beta", file_name="prov.filter", epoch=900001)

        results = kb_store.search_engine.search("filter", author="filter-a")
        bm25_results = [r for r in results if r.source == "bm25"]
        assert all(r.author == "filter-a" for r in bm25_results)

    def test_search_passes_hostname_filter(self, kb_store):
        kb_store.create_file("prov.hfilter", "hostname filter", ["hostname"])
        kb_store.add_entry(content="@hostname: special-host\nhostname filter content", file_name="prov.hfilter", epoch=950000)

        results = kb_store.search_engine.search("hostname", hostname="special-host")
        bm25_results = [r for r in results if r.source == "bm25"]
        assert all(r.hostname == "special-host" for r in bm25_results)

    def test_merged_results_preserve_provenance(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "merge-test")
        kb_store.create_file("prov.merge", "merge test", ["merge"])
        kb_store.add_entry(content="merge provenance test", file_name="prov.merge", epoch=960000)

        results = kb_store.search("merge provenance")
        for r in results:
            if r.source == "bm25" or "bm25" in r.source:
                assert r.author == "merge-test"
                assert r.hostname == socket.gethostname()


# ---------------------------------------------------------------------------
# mcp_server.py
# ---------------------------------------------------------------------------

class TestMcpProvenance:
    def test_search_output_includes_provenance(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "mcp-test")
        kb_store.create_file("prov.mcp", "mcp provenance test", ["mcp"])
        kb_store.add_entry(content="mcp provenance entry", file_name="prov.mcp", epoch=1100000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search("mcp provenance", ctx=ctx))
        assert isinstance(result, list)
        assert len(result) >= 1
        assert "author" in result[0]
        assert "hostname" in result[0]

    def test_search_filter_by_author(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "client-a")
        kb_store.create_file("prov.mcpf", "mcp filter test", ["filter"])
        kb_store.add_entry(content="mcp filter entry alpha", file_name="prov.mcpf", epoch=1200000)
        monkeypatch.setenv("HKB_SOURCE", "client-b")
        kb_store.add_entry(content="mcp filter entry beta", file_name="prov.mcpf", epoch=1200001)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search("mcp filter", author="client-a", ctx=ctx))
        # BM25 results should only contain client-a
        bm25 = [r for r in result if "bm25" in r.get("source", "")]
        assert all(r["author"] == "client-a" for r in bm25)

    def test_search_filter_by_hostname(self, kb_store):
        kb_store.create_file("prov.mcph", "mcp hostname", ["hostname"])
        kb_store.add_entry(content="@hostname: test-host\nmcp hostname entry", file_name="prov.mcph", epoch=1300000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search("mcp hostname", hostname="test-host", ctx=ctx))
        bm25 = [r for r in result if "bm25" in r.get("source", "")]
        assert all(r["hostname"] == "test-host" for r in bm25)

    def test_recent_output_includes_provenance(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "recent-mcp")
        kb_store.create_file("prov.rmcp", "recent mcp", ["recent"])
        kb_store.add_entry(content="recent mcp entry", file_name="prov.rmcp", epoch=1400000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search(mode="recent", ctx=ctx))
        assert isinstance(result, list)
        assert len(result) >= 1
        assert "author" in result[0]
        assert "hostname" in result[0]

    def test_recent_filter_by_author(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "rec-a")
        kb_store.create_file("prov.rfa", "recent filter", ["filter"])
        kb_store.add_entry(content="recent filter one", file_name="prov.rfa", epoch=1500000)
        monkeypatch.setenv("HKB_SOURCE", "rec-b")
        kb_store.add_entry(content="recent filter two", file_name="prov.rfa", epoch=1500001)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search(mode="recent", author="rec-a", ctx=ctx))
        assert len(result) == 1
        assert result[0]["author"] == "rec-a"

    def test_briefing_provenance_summary(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "briefing-client")
        kb_store.create_file("prov.brief", "briefing test", ["briefing"])
        kb_store.add_entry(content="briefing provenance", file_name="prov.brief", epoch=1600000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        summary = result["summary"]
        assert "provenance" in summary
        assert "briefing-client" in summary["provenance"]["authors"]
        assert socket.gethostname() in summary["provenance"]["hostnames"]

    def test_briefing_recent_items_have_provenance(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "brief-items")
        kb_store.create_file("prov.bi", "briefing items", ["items"])
        kb_store.add_entry(content="briefing item test", file_name="prov.bi", epoch=1700000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        for file_name, items in result["recent_activity"].items():
            for item in items:
                assert "author" in item
                assert "hostname" in item

    def test_session_review_author_distribution(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "review-a")
        kb_store.create_file("prov.rev", "review test", ["review"])
        kb_store.add_entry(content="review entry one", file_name="prov.rev", epoch=1800000)
        monkeypatch.setenv("HKB_SOURCE", "review-b")
        kb_store.add_entry(content="review entry two", file_name="prov.rev", epoch=1800001)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_session(action="review", ctx=ctx))
        assert "author_distribution" in result
        assert result["author_distribution"].get("review-a", 0) >= 1
        assert result["author_distribution"].get("review-b", 0) >= 1

    def test_session_review_hostname_distribution(self, kb_store, monkeypatch):
        kb_store.create_file("prov.revh", "review hostname", ["review"])
        kb_store.add_entry(content="review hostname entry", file_name="prov.revh", epoch=1900000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_session(action="review", ctx=ctx))
        assert "hostname_distribution" in result
        hostname = socket.gethostname()
        assert result["hostname_distribution"].get(hostname, 0) >= 1

    def test_session_review_per_item_provenance(self, kb_store, monkeypatch):
        monkeypatch.setenv("HKB_SOURCE", "item-prov")
        kb_store.create_file("prov.rpi", "review per item", ["review"])
        kb_store.add_entry(content="per item provenance", file_name="prov.rpi", epoch=2000000)

        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_session(action="review", ctx=ctx))
        for group_key, items in result.get("entries_by_file", {}).items():
            for item in items:
                assert "author" in item
                assert "hostname" in item
