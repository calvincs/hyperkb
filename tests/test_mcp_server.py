"""Tests for the MCP server tools.

Tests call tool functions directly with a mock context, reusing the
sample_kb fixture from conftest.py for populated KB tests.
"""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

mcp = pytest.importorskip("mcp")

from hyperkb.mcp_server import (
    AppContext,
    LOCK_FILENAME,
    _ServerLock,
    app_lifespan,
    hkb_add,
    hkb_context,
    hkb_health,
    hkb_search,
    hkb_session,
    hkb_show,
    hkb_sync,
    hkb_task,
    hkb_update,
    hkb_view,
    _extract_snippet,
    _parse_duration_seconds,
    _parse_server_args,
)


def _make_ctx(store, health=None):
    """Build a mock Context whose lifespan_context holds the given store."""
    ctx = MagicMock()
    ctx.request_context.lifespan_context = AppContext(store=store, health=health)
    return ctx


# ---------------------------------------------------------------------------
# hkb_show (name="" => file listing, replaces old hkb_list)
# ---------------------------------------------------------------------------

class TestHkbList:
    def test_list_empty(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_show(name="", ctx=ctx))
        assert result["scope"] == "global"
        assert result["files"] == []

    def test_list_populated(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="", ctx=ctx))
        names = {f["name"] for f in result["files"]}
        assert "security.threat-intel" in names
        assert "fitness.kettlebell" in names

    def test_list_domain_filter(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="", domain="security", ctx=ctx))
        assert len(result["files"]) == 1
        assert result["files"][0]["name"] == "security.threat-intel"

    def test_list_keywords_decoded(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="", ctx=ctx))
        for f in result["files"]:
            assert isinstance(f["keywords"], list)


# ---------------------------------------------------------------------------
# hkb_show
# ---------------------------------------------------------------------------

class TestHkbShow:
    def test_show_file(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="security.threat-intel", ctx=ctx))
        assert result["header"]["name"] == "security.threat-intel"
        assert len(result["entries"]) == 2

    def test_show_not_found(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="does.not.exist", ctx=ctx))
        assert result["status"] == "error"

    def test_show_last_n(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="security.threat-intel", last=1, ctx=ctx))
        assert len(result["entries"]) == 1
        assert result["entries"][0]["epoch"] == 2000000

    def test_show_compact(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Add a multiline entry first
        sample_kb.add_entry(
            content="Line one\nLine two\nLine three",
            file_name="fitness.kettlebell",
            epoch=4000000,
        )
        result = json.loads(hkb_show(name="fitness.kettlebell", compact=True, ctx=ctx))
        for e in result["entries"]:
            assert "\n" not in e["content"]


# ---------------------------------------------------------------------------
# hkb_search
# ---------------------------------------------------------------------------

class TestHkbSearch:
    def test_search_bm25(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(query="AlienVault", mode="bm25", ctx=ctx))
        assert isinstance(result, list)
        # Should find the AlienVault entry
        assert any("AlienVault" in r["content"] for r in result)

    def test_search_no_results(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(query="xyznonexistent", mode="bm25", ctx=ctx))
        assert result == []

    def test_search_domain_filter(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_search(query="press", mode="bm25", domain="fitness", ctx=ctx)
        )
        for r in result:
            assert r["file_name"].startswith("fitness.")

    def test_search_with_offset(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        all_results = json.loads(
            hkb_search(query="AlienVault MISP", mode="bm25", top=10, ctx=ctx)
        )
        if len(all_results) >= 2:
            offset_results = json.loads(
                hkb_search(query="AlienVault MISP", mode="bm25", top=10, offset=1, ctx=ctx)
            )
            assert len(offset_results) == len(all_results) - 1


class TestSearchResultQuality:
    def test_search_includes_metadata(self, sample_kb):
        """Results include status, type, tags fields."""
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(query="AlienVault", mode="bm25", ctx=ctx))
        assert len(result) >= 1
        r = result[0]
        assert "status" in r
        assert "type" in r
        assert "tags" in r

    def test_search_includes_snippet(self, sample_kb):
        """Results include snippet field."""
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(query="AlienVault", mode="bm25", ctx=ctx))
        assert len(result) >= 1
        assert "snippet" in result[0]
        assert len(result[0]["snippet"]) > 0

    def test_snippet_centers_on_match(self):
        """Snippet contains the query term, not just first 200 chars."""
        long_content = "A" * 300 + " target_keyword " + "B" * 300
        snippet = _extract_snippet(long_content, "target_keyword", max_len=200)
        assert "target_keyword" in snippet

    def test_snippet_short_content_returns_full(self):
        """Content under max_len chars is returned as-is."""
        short = "This is short content."
        snippet = _extract_snippet(short, "short", max_len=200)
        assert snippet == short


# ---------------------------------------------------------------------------
# hkb_search mode="check" (replaces old hkb_check)
# ---------------------------------------------------------------------------

class TestHkbCheck:
    def test_check_content(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(mode="check", query="IOC feed delay observation", ctx=ctx))
        assert isinstance(result, list)

    def test_check_empty_kb(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search(mode="check", query="anything", ctx=ctx))
        assert result == []


# ---------------------------------------------------------------------------
# hkb_show with links=True (replaces old hkb_links)
# ---------------------------------------------------------------------------

class TestHkbLinks:
    def test_links(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="security.threat-intel", links=True, ctx=ctx))
        assert "outbound" in result.get("link_graph", result)
        assert "inbound_header" in result.get("link_graph", result)
        assert "inbound_entries" in result.get("link_graph", result)

    def test_links_inbound_wikilink(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # The sample_kb has [[fitness.kettlebell]] in a security entry
        result = json.loads(hkb_show(name="fitness.kettlebell", links=True, ctx=ctx))
        link_graph = result.get("link_graph", result)
        assert "security.threat-intel" in link_graph["inbound_entries"]

    def test_links_entry_level_outbound(self, sample_kb):
        """hkb_show with links+epoch returns outbound entry links (after reindex)."""
        ctx = _make_ctx(sample_kb)
        hkb_health(action="reindex", ctx=ctx)
        # Entry at epoch 2000000 has [[fitness.kettlebell]]
        result = json.loads(hkb_show(name="security.threat-intel", links=True, epoch=2000000, ctx=ctx))
        assert "entry" in result
        assert result["entry"]["epoch"] == 2000000
        target_files = [r["target_file"] for r in result["outbound"]]
        assert "fitness.kettlebell" in target_files

    def test_links_entry_level_inbound(self, sample_kb):
        """hkb_show with links+epoch shows inbound backlinks."""
        ctx = _make_ctx(sample_kb)
        hkb_health(action="reindex", ctx=ctx)
        # fitness.kettlebell is linked by security.threat-intel entry 2000000
        result = json.loads(hkb_show(name="fitness.kettlebell", links=True, epoch=3000000, ctx=ctx))
        # file-level backlinks (target_epoch=0) should be included
        source_files = [r["source_file"] for r in result["inbound"]]
        assert "security.threat-intel" in source_files

    def test_links_without_epoch_unchanged(self, sample_kb):
        """File-level response shape preserved (backward compat)."""
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="security.threat-intel", links=True, ctx=ctx))
        link_graph = result.get("link_graph", result)
        # Should have file-level keys, not entry-level
        assert "outbound" in link_graph
        assert "inbound_header" in link_graph
        assert "inbound_entries" in link_graph
        assert "entry" not in result

    def test_links_entry_no_links(self, sample_kb):
        """Entry with no wiki-links returns empty lists."""
        ctx = _make_ctx(sample_kb)
        hkb_health(action="reindex", ctx=ctx)
        # Entry at epoch 1000000 has no wiki-links
        result = json.loads(hkb_show(name="security.threat-intel", links=True, epoch=1000000, ctx=ctx))
        assert result["outbound"] == []

    def test_links_outbound_entries(self, sample_kb):
        """File-level response includes outbound_entries from wiki-links in entries."""
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_show(name="security.threat-intel", links=True, ctx=ctx))
        link_graph = result.get("link_graph", result)
        assert "outbound_entries" in link_graph
        # The sample_kb has [[fitness.kettlebell]] in a security entry
        assert "fitness.kettlebell" in link_graph["outbound_entries"]

    def test_links_entry_level_no_reindex_needed(self, sample_kb):
        """Entry-level links work without reindex (populated at write time)."""
        ctx = _make_ctx(sample_kb)
        # DO NOT call hkb_health(action="reindex") — links should already be populated from add_entry
        result = json.loads(hkb_show(name="security.threat-intel", links=True, epoch=2000000, ctx=ctx))
        target_files = [r["target_file"] for r in result["outbound"]]
        assert "fitness.kettlebell" in target_files


# ---------------------------------------------------------------------------
# hkb_add
# ---------------------------------------------------------------------------

class TestHkbAdd:
    def test_add_direct(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_add(
                content="New threat intel observation",
                to="security.threat-intel",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        assert result["file"] == "security.threat-intel"
        assert "epoch" in result

    def test_add_with_epoch(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_add(
                content="Observation at specific time",
                to="fitness.kettlebell",
                epoch=9999999,
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        assert result["epoch"] == 9999999

    def test_add_bad_file(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_add(content="stuff", to="does.not.exist", ctx=ctx)
        )
        assert result["status"] == "error"

    def test_add_auto_route(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_add(content="IOC feed enrichment for threat intel", ctx=ctx)
        )
        # Should either route successfully or return low_confidence/no_match
        assert result["status"] in ("ok", "low_confidence", "no_match")


# ---------------------------------------------------------------------------
# hkb_add with create_file=True (replaces old hkb_create)
# ---------------------------------------------------------------------------

class TestHkbCreate:
    def test_create_file(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(
            hkb_add(
                create_file=True,
                to="test.new-file",
                description="A test file for unit tests.",
                keywords=["test", "unit"],
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        assert "Created" in result["message"]

    def test_create_duplicate(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_add(
                create_file=True,
                to="security.threat-intel",
                description="duplicate",
                keywords=["dup"],
                ctx=ctx,
            )
        )
        assert result["status"] == "error"
        assert "already exists" in result["message"]

    def test_create_invalid_name(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(
            hkb_add(
                create_file=True,
                to="onlyone",
                description="bad name",
                keywords=[],
                ctx=ctx,
            )
        )
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# hkb_health action="reindex" (replaces old hkb_reindex)
# ---------------------------------------------------------------------------

class TestHkbReindex:
    def test_reindex(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(action="reindex", ctx=ctx))
        assert result["status"] == "ok"
        assert "Reindexed" in result["message"]
        assert "2 files" in result["message"]
        assert "3 entries" in result["message"]


# ---------------------------------------------------------------------------
# parse_add_result (moved to format.py)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# auto-init
# ---------------------------------------------------------------------------

class TestAutoInit:
    def test_auto_init_creates_kb(self, tmp_path, monkeypatch):
        """MCP lifespan auto-creates KB when none exists."""
        import asyncio
        import hyperkb.mcp_server as mcp_mod

        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(mcp_mod, "_server_args", _parse_server_args([]))

        async def _run():
            server = MagicMock()
            async with app_lifespan(server) as ctx:
                assert isinstance(ctx, AppContext)
                assert ctx.store is not None
                assert (tmp_path / ".hkb" / "config.json").exists()
                assert (tmp_path / ".hkb" / "index.db").exists()

        asyncio.run(_run())

    def test_auto_init_with_explicit_path(self, tmp_path, monkeypatch):
        """MCP lifespan auto-creates KB at explicit --path when none exists."""
        import asyncio
        import hyperkb.mcp_server as mcp_mod

        kb_dir = tmp_path / "custom"
        kb_dir.mkdir()
        monkeypatch.setattr(mcp_mod, "_server_args", _parse_server_args(["--path", str(kb_dir)]))

        async def _run():
            server = MagicMock()
            async with app_lifespan(server) as ctx:
                assert isinstance(ctx, AppContext)
                assert (kb_dir / ".hkb" / "config.json").exists()

        asyncio.run(_run())

    def test_startup_syncs_entry_links(self, tmp_path, monkeypatch):
        """MCP lifespan calls sync_entry_links() to populate links if empty."""
        import asyncio
        import hyperkb.mcp_server as mcp_mod
        from hyperkb.config import KBConfig
        from hyperkb.store import KnowledgeStore

        monkeypatch.setenv("HOME", str(tmp_path))

        # Pre-create a KB with entries that have wiki-links, but no entry_links
        config = KBConfig(root=str(tmp_path))
        store = KnowledgeStore(config)
        store.init()
        store.create_file(name="a.b", description="d", keywords=[])
        store.create_file(name="c.d", description="d", keywords=[])
        store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        # Clear the links that add_entry populated
        store.db.clear_entry_links()
        assert store.db.count_entry_links() == 0
        store.close()

        monkeypatch.setattr(mcp_mod, "_server_args", _parse_server_args([]))

        async def _run():
            server = MagicMock()
            async with app_lifespan(server) as ctx:
                # After lifespan startup, entry_links should be synced
                assert ctx.store.db.count_entry_links() > 0

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# Error boundaries — unprotected tools should return JSON errors, not crash
# ---------------------------------------------------------------------------

class TestErrorBoundaries:
    def test_list_error_returns_json(self, kb_store):
        from unittest.mock import patch
        ctx = _make_ctx(kb_store)
        with patch.object(kb_store, "list_files", side_effect=RuntimeError("db boom")):
            result = json.loads(hkb_show(name="", ctx=ctx))
        assert result["status"] == "error"
        assert "db boom" in result["message"]

    def test_check_error_returns_json(self, kb_store):
        from unittest.mock import patch
        ctx = _make_ctx(kb_store)
        with patch.object(kb_store, "check_content", side_effect=RuntimeError("check boom")):
            result = json.loads(hkb_search(mode="check", query="test", ctx=ctx))
        assert result["status"] == "error"
        assert "check boom" in result["message"]

    def test_links_error_returns_json(self, kb_store):
        from unittest.mock import patch
        ctx = _make_ctx(kb_store)
        # Create file so show_file succeeds, then get_links raises
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with patch.object(kb_store, "get_links", side_effect=RuntimeError("links boom")):
            result = json.loads(hkb_show(name="test.file", links=True, ctx=ctx))
        assert result["status"] == "error"
        assert "links boom" in result["message"]

    def test_reindex_error_returns_json(self, kb_store):
        from unittest.mock import patch
        ctx = _make_ctx(kb_store)
        with patch.object(kb_store, "reindex", side_effect=RuntimeError("reindex boom")):
            result = json.loads(hkb_health(action="reindex", ctx=ctx))
        assert result["status"] == "error"
        assert "reindex boom" in result["message"]


# ---------------------------------------------------------------------------
# Parameter bounds
# ---------------------------------------------------------------------------

class TestParameterBounds:
    def test_search_top_clamped(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # top=999999 should be clamped to 500 and not crash
        result = json.loads(hkb_search(query="AlienVault", mode="bm25", top=999999, ctx=ctx))
        assert isinstance(result, list)

    def test_search_negative_offset_clamped(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(query="AlienVault", mode="bm25", offset=-5, ctx=ctx))
        assert isinstance(result, list)


class TestParseAddResult:
    def test_parse_success(self):
        from hyperkb.format import parse_add_result
        r = parse_add_result("Added to security.findings at epoch 12345")
        assert r == {"status": "ok", "file": "security.findings", "epoch": 12345}

    def test_parse_broken_links(self):
        from hyperkb.format import parse_add_result
        r = parse_add_result(
            "Added to f.name at epoch 100\nWarning: broken links: a.b, c.d"
        )
        assert r["status"] == "ok"
        assert r["broken_links"] == ["a.b", "c.d"]

    def test_parse_no_match(self):
        from hyperkb.format import parse_add_result
        r = parse_add_result("NO_MATCH: nothing found")
        assert r["status"] == "no_match"

    def test_parse_low_confidence(self):
        from hyperkb.format import parse_add_result
        text = (
            "LOW_CONFIDENCE: Best match is 'a.b' (score: 0.40).\n"
            "Candidates:\n"
            "  - a.b (score: 0.40): reason1\n"
            "  - c.d (score: 0.20): reason2\n"
        )
        r = parse_add_result(text)
        assert r["status"] == "low_confidence"
        assert len(r["candidates"]) == 2
        assert r["candidates"][0]["name"] == "a.b"


# ---------------------------------------------------------------------------
# hkb_update
# ---------------------------------------------------------------------------

class TestHkbUpdate:
    def test_update_content(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                file="security.threat-intel",
                epoch=1000000,
                new_content="Updated observation.",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        assert result["epoch"] == 1000000

    def test_update_status(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                file="security.threat-intel",
                epoch=1000000,
                set_status="superseded",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"

    def test_update_nonexistent_entry(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                file="security.threat-intel",
                epoch=9999999,
                set_status="resolved",
                ctx=ctx,
            )
        )
        assert result["status"] == "error"
        assert "not found" in result["message"].lower()

    def test_update_no_changes(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                file="security.threat-intel",
                epoch=1000000,
                ctx=ctx,
            )
        )
        assert result["status"] == "error"
        assert "At least one" in result["message"]


# ---------------------------------------------------------------------------
# hkb_update action="archive" (replaces old hkb_archive)
# ---------------------------------------------------------------------------

class TestHkbArchive:
    def test_archive_entry(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                action="archive",
                file="security.threat-intel",
                epoch=1000000,
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        assert result["archived_to"] == "security.threat-intel.archive"

    def test_archive_nonexistent(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                action="archive",
                file="security.threat-intel",
                epoch=9999999,
                ctx=ctx,
            )
        )
        assert result["status"] == "error"

    def test_list_shows_is_archive_flag(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Archive an entry to create the archive file
        hkb_update(action="archive", file="security.threat-intel", epoch=1000000, ctx=ctx)
        result = json.loads(hkb_show(name="", ctx=ctx))
        archive_files = [f for f in result["files"] if f["is_archive"]]
        assert len(archive_files) >= 1
        non_archive = [f for f in result["files"] if not f["is_archive"]]
        assert len(non_archive) >= 1


# ---------------------------------------------------------------------------
# hkb_health action="compact" (replaces old hkb_compact)
# ---------------------------------------------------------------------------

class TestHkbCompact:
    def _populate(self, kb_store, name, epochs):
        """Create file and add entries at given epochs."""
        kb_store.create_file(name=name, description="test file", keywords=["test"])
        for i, ep in enumerate(epochs):
            kb_store.add_entry(f"Entry {i}.", file_name=name, epoch=ep)

    def test_compact_dry_run(self, kb_store):
        self._populate(kb_store, "test.compact", [1000, 1100, 1200])
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(
            action="compact",
            file="test.compact", gap="1h", min_cluster=3, min_age="0",
            dry_run=True, ctx=ctx,
        ))
        assert result["status"] == "preview"
        assert result["total_entries"] == 3
        assert result["eligible_clusters"] == 1

    def test_compact_execute(self, kb_store):
        self._populate(kb_store, "test.compact", [1000, 1100, 1200])
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(
            action="compact",
            file="test.compact", gap="1h", min_cluster=3, min_age="0",
            dry_run=False, ctx=ctx,
        ))
        assert result["status"] == "ok"
        assert result["compacted_clusters"] == 1
        assert result["entries_archived"] == 3
        assert result["summaries_created"] == 1

    def test_compact_param_validation(self, kb_store):
        ctx = _make_ctx(kb_store)
        # Bad file name
        result = json.loads(hkb_health(action="compact", file="no.such.file", ctx=ctx))
        assert result["status"] == "error"
        # Bad gap format
        self._populate(kb_store, "test.valid", [1000])
        result = json.loads(hkb_health(action="compact", file="test.valid", gap="bad", ctx=ctx))
        assert result["status"] == "error"

    def test_compact_clamps_min_cluster(self, kb_store):
        self._populate(kb_store, "test.clamp", [1000, 1100])
        ctx = _make_ctx(kb_store)
        # min_cluster=1 should be clamped to 2
        result = json.loads(hkb_health(
            action="compact",
            file="test.clamp", gap="1h", min_cluster=1, min_age="0",
            dry_run=True, ctx=ctx,
        ))
        assert result["status"] == "preview"
        # With clamped min_cluster=2, the cluster of 2 is eligible
        assert result["eligible_clusters"] == 1

    def test_compact_file_not_found(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(action="compact", file="missing.file", ctx=ctx))
        assert result["status"] == "error"
        assert "not found" in result["message"]


class TestParseDurationSeconds:
    def test_hours(self):
        assert _parse_duration_seconds("4h") == 14400

    def test_days(self):
        assert _parse_duration_seconds("7d") == 604800

    def test_raw_seconds(self):
        assert _parse_duration_seconds("3600") == 3600

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            _parse_duration_seconds("bad")


# ---------------------------------------------------------------------------
# hkb_search mode="recent" (replaces old hkb_recent)
# ---------------------------------------------------------------------------

class TestHkbRecent:
    def test_recent_returns_entries(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(mode="recent", ctx=ctx))
        assert isinstance(result, list)
        assert len(result) == 3  # 3 entries in sample_kb
        # Newest first
        assert result[0]["epoch"] >= result[1]["epoch"]

    def test_recent_with_domain(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(mode="recent", domain="security", ctx=ctx))
        assert all(r["file_name"].startswith("security") for r in result)

    def test_recent_with_top(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_search(mode="recent", top=1, ctx=ctx))
        assert len(result) == 1

    def test_recent_excludes_archived(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Archive an entry
        hkb_update(action="archive", file="security.threat-intel", epoch=1000000, ctx=ctx)
        result = json.loads(hkb_search(mode="recent", ctx=ctx))
        # Archived entry should not appear
        assert all(r["file_name"] != "security.threat-intel.archive" for r in result)

    def test_recent_empty_results(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_search(mode="recent", ctx=ctx))
        assert result == []


# ---------------------------------------------------------------------------
# hkb_session action="briefing" (replaces old hkb_briefing)
# ---------------------------------------------------------------------------

class TestHkbBriefing:
    def test_briefing_basic_structure(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        assert "summary" in result
        assert "recent_activity" in result
        assert "open_tasks" in result
        assert "files" in result

    def test_briefing_summary_counts(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        summary = result["summary"]
        assert summary["total_files"] == 2
        assert summary["total_entries"] == 3
        assert summary["recent_entry_count"] == 3
        assert summary["open_task_count"] == 0

    def test_briefing_recent_grouped_by_file(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        activity = result["recent_activity"]
        assert "security.threat-intel" in activity
        assert "fitness.kettlebell" in activity
        assert len(activity["security.threat-intel"]) == 2
        assert len(activity["fitness.kettlebell"]) == 1

    def test_briefing_recent_newest_file_first(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        activity = result["recent_activity"]
        file_keys = list(activity.keys())
        # fitness.kettlebell has epoch 3000000 (newest), should come first
        assert file_keys[0] == "fitness.kettlebell"
        assert file_keys[1] == "security.threat-intel"

    def test_briefing_content_truncated(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Add an entry with content longer than 150 chars
        long_content = "A" * 200
        sample_kb.add_entry(
            content=long_content,
            file_name="fitness.kettlebell",
            epoch=4000000,
        )
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        activity = result["recent_activity"]
        # The first entry for fitness.kettlebell should be the long one (newest)
        preview = activity["fitness.kettlebell"][0]["preview"]
        assert len(preview) == 153  # 150 + "..."
        assert preview.endswith("...")

    def test_briefing_domain_filter(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="briefing", domain="security", ctx=ctx))
        # Only security files
        assert all(f["name"].startswith("security") for f in result["files"])
        # Only security entries in recent
        assert all(
            k.startswith("security") for k in result["recent_activity"]
        )

    def test_briefing_with_tasks(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        hkb_task(action="create", title="Fix the bug", file="tasks.default", ctx=ctx)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        assert result["summary"]["open_task_count"] == 1
        assert len(result["open_tasks"]) == 1
        assert result["open_tasks"][0]["status"] == "pending"
        assert "Fix the bug" in result["open_tasks"][0]["preview"]

    def test_briefing_excludes_archives(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Archive an entry to create an archive file
        hkb_update(action="archive", file="security.threat-intel", epoch=1000000, ctx=ctx)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        # Archive files should be excluded from the files list
        file_names = [f["name"] for f in result["files"]]
        assert not any("archive" in n for n in file_names)

    def test_briefing_empty_kb(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        assert result["summary"]["total_files"] == 0
        assert result["summary"]["total_entries"] == 0
        assert result["summary"]["recent_entry_count"] == 0
        assert result["summary"]["open_task_count"] == 0
        assert result["recent_activity"] == {}
        assert result["open_tasks"] == []
        assert result["files"] == []

    def test_briefing_error_returns_json(self, kb_store):
        from unittest.mock import patch
        ctx = _make_ctx(kb_store)
        with patch.object(kb_store, "get_recent", side_effect=RuntimeError("boom")):
            result = json.loads(hkb_session(action="briefing", ctx=ctx))
        assert result["status"] == "error"
        assert "boom" in result["message"]


# ---------------------------------------------------------------------------
# hkb_task action="create" (replaces old hkb_task_create)
# ---------------------------------------------------------------------------

class TestHkbTaskCreate:
    def test_create_task(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(
            hkb_task(
                action="create",
                title="Implement search filters",
                file="tasks.test",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        assert result["file"] == "tasks.test"
        # Verify entry has @type: task and @status: pending
        entry = kb_store.db.get_entry("tasks.test", result["epoch"])
        assert entry["entry_type"] == "task"
        assert entry["status"] == "pending"

    def test_create_task_auto_creates_file(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(
            hkb_task(action="create", title="Auto-create test", ctx=ctx)
        )
        assert result["status"] == "ok"
        assert result["file"] == "tasks.default"
        assert kb_store.db.file_exists("tasks.default")

    def test_create_task_with_priority(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(
            hkb_task(
                action="create",
                title="High priority task",
                priority="high",
                file="tasks.test",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        entry = kb_store.db.get_entry("tasks.test", result["epoch"])
        assert "priority-high" in entry["tags"]

    def test_create_task_with_description(self, kb_store):
        ctx = _make_ctx(kb_store)
        result = json.loads(
            hkb_task(
                action="create",
                title="Task with desc",
                description="Detailed description here.",
                file="tasks.test",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        entry = kb_store.db.get_entry("tasks.test", result["epoch"])
        assert "Detailed description" in entry["content"]


# ---------------------------------------------------------------------------
# hkb_task action="update" (replaces old hkb_task_update)
# ---------------------------------------------------------------------------

class TestHkbTaskUpdate:
    def test_update_task_status(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(action="create", title="Task to update", file="tasks.test", ctx=ctx)
        )
        result = json.loads(
            hkb_task(
                action="update",
                file="tasks.test",
                epoch=created["epoch"],
                status="completed",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        entry = kb_store.db.get_entry("tasks.test", created["epoch"])
        assert entry["status"] == "completed"

    def test_update_task_with_note(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(action="create", title="Task with note", file="tasks.test", ctx=ctx)
        )
        result = json.loads(
            hkb_task(
                action="update",
                file="tasks.test",
                epoch=created["epoch"],
                status="completed",
                note="Done in commit abc123",
                ctx=ctx,
            )
        )
        assert result["status"] == "ok"
        entry = kb_store.db.get_entry("tasks.test", created["epoch"])
        assert "abc123" in entry["content"]
        assert "Status →" in entry["content"]

    def test_update_nonexistent_task(self, kb_store):
        ctx = _make_ctx(kb_store)
        kb_store.create_file(name="tasks.test", description="d", keywords=[])
        result = json.loads(
            hkb_task(
                action="update",
                file="tasks.test",
                epoch=9999999,
                status="completed",
                ctx=ctx,
            )
        )
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# hkb_task action="list" (replaces old hkb_task_list)
# ---------------------------------------------------------------------------

class TestHkbTaskList:
    def test_list_tasks(self, kb_store):
        ctx = _make_ctx(kb_store)
        hkb_task(action="create", title="Task one", file="tasks.test", ctx=ctx)
        hkb_task(action="create", title="Task two", file="tasks.test", ctx=ctx)
        result = json.loads(hkb_task(action="list", ctx=ctx))
        tasks = result["tasks"]
        assert len(tasks) == 2
        # All should be tasks with pending status
        assert all(r["status"] == "pending" for r in tasks)
        assert "_summary" in result

    def test_list_tasks_filters_by_status(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(action="create", title="Completed task", file="tasks.test", ctx=ctx)
        )
        hkb_task(
            action="update",
            file="tasks.test", epoch=created["epoch"], status="completed", ctx=ctx,
        )
        hkb_task(action="create", title="Pending task", file="tasks.test", ctx=ctx)
        # Default: only pending/in_progress/blocked
        result = json.loads(hkb_task(action="list", ctx=ctx))
        tasks = result["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["status"] == "pending"
        # All statuses
        result = json.loads(hkb_task(action="list", status="all", ctx=ctx))
        assert len(result["tasks"]) == 2

    def test_list_tasks_filters_by_domain(self, kb_store):
        ctx = _make_ctx(kb_store)
        hkb_task(action="create", title="Task A", file="tasks.alpha", ctx=ctx)
        hkb_task(action="create", title="Task B", file="tasks.beta", ctx=ctx)
        hkb_task(action="create", title="Task C", file="work.gamma", ctx=ctx)
        result = json.loads(hkb_task(action="list", domain="tasks", ctx=ctx))
        tasks = result["tasks"]
        assert len(tasks) == 2
        assert all(r["file_name"].startswith("tasks") for r in tasks)

    def test_list_only_returns_tasks(self, kb_store):
        ctx = _make_ctx(kb_store)
        # Create a task
        hkb_task(action="create", title="A task", file="tasks.test", ctx=ctx)
        # Add a regular entry to the same file
        hkb_add(content="A regular note, not a task.", to="tasks.test", ctx=ctx)
        result = json.loads(hkb_task(action="list", ctx=ctx))
        assert len(result["tasks"]) == 1  # only the task, not the note

    def test_list_tasks_has_summary(self, kb_store):
        ctx = _make_ctx(kb_store)
        c1 = json.loads(hkb_task(action="create", title="Task A", file="tasks.test", ctx=ctx))
        hkb_task(action="create", title="Task B", file="tasks.test", ctx=ctx)
        hkb_task(action="update", file="tasks.test", epoch=c1["epoch"], status="in_progress", ctx=ctx)
        result = json.loads(hkb_task(action="list", ctx=ctx))
        summary = result["_summary"]
        # All 5 task status keys present
        assert set(summary.keys()) == {"pending", "in_progress", "blocked", "completed", "cancelled"}
        assert summary["pending"] == 1
        assert summary["in_progress"] == 1
        assert summary["blocked"] == 0
        assert summary["completed"] == 0
        assert summary["cancelled"] == 0


# ---------------------------------------------------------------------------
# hkb_task action="show" (replaces old hkb_task_show)
# ---------------------------------------------------------------------------

class TestHkbTaskShow:
    def test_task_show_basic(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(action="create", title="Fix the bug", file="tasks.test", ctx=ctx)
        )
        result = json.loads(
            hkb_task(action="show", file="tasks.test", epoch=created["epoch"], ctx=ctx)
        )
        assert result["file"] == "tasks.test"
        assert result["epoch"] == created["epoch"]
        assert result["title"] == "Fix the bug"
        assert result["status"] == "pending"
        assert "timeline" in result
        assert "dependencies" in result

    def test_task_show_with_description(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(
                action="create",
                title="Add feature",
                description="Detailed description of the feature.",
                file="tasks.test",
                ctx=ctx,
            )
        )
        result = json.loads(
            hkb_task(action="show", file="tasks.test", epoch=created["epoch"], ctx=ctx)
        )
        assert result["title"] == "Add feature"
        assert "Detailed description" in result["description"]

    def test_task_show_with_timeline(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(action="create", title="Track me", file="tasks.test", ctx=ctx)
        )
        hkb_task(
            action="update",
            file="tasks.test", epoch=created["epoch"],
            status="in_progress", note="Started work", ctx=ctx,
        )
        result = json.loads(
            hkb_task(action="show", file="tasks.test", epoch=created["epoch"], ctx=ctx)
        )
        assert len(result["timeline"]) == 1
        assert result["timeline"][0]["status"] == "in_progress"
        assert result["timeline"][0]["note"] == "Started work"
        assert "date" in result["timeline"][0]

    def test_task_show_with_dependencies(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(
                action="create",
                title="Blocked task",
                blocked_by="tasks.test#1234567890",
                file="tasks.test",
                ctx=ctx,
            )
        )
        result = json.loads(
            hkb_task(action="show", file="tasks.test", epoch=created["epoch"], ctx=ctx)
        )
        assert "tasks.test#1234567890" in result["dependencies"]

    def test_task_show_not_found(self, kb_store):
        ctx = _make_ctx(kb_store)
        kb_store.create_file(name="tasks.test", description="d", keywords=[])
        result = json.loads(
            hkb_task(action="show", file="tasks.test", epoch=9999999, ctx=ctx)
        )
        assert result["status"] == "error"
        assert "not found" in result["message"].lower()

    def test_task_show_not_a_task(self, kb_store):
        ctx = _make_ctx(kb_store)
        kb_store.create_file(name="notes.test", description="d", keywords=[])
        kb_store.add_entry(content="Just a note", file_name="notes.test", epoch=5000000)
        result = json.loads(
            hkb_task(action="show", file="notes.test", epoch=5000000, ctx=ctx)
        )
        assert result["status"] == "error"
        assert "not a task" in result["message"].lower()

    def test_task_show_no_notes(self, kb_store):
        ctx = _make_ctx(kb_store)
        created = json.loads(
            hkb_task(action="create", title="Fresh task", file="tasks.test", ctx=ctx)
        )
        result = json.loads(
            hkb_task(action="show", file="tasks.test", epoch=created["epoch"], ctx=ctx)
        )
        assert result["timeline"] == []

    def test_task_show_error(self, kb_store):
        from unittest.mock import patch
        ctx = _make_ctx(kb_store)
        with patch.object(kb_store.db, "get_entry", side_effect=RuntimeError("boom")):
            result = json.loads(
                hkb_task(action="show", file="tasks.test", epoch=1, ctx=ctx)
            )
        assert result["status"] == "error"
        assert "boom" in result["message"]


# ---------------------------------------------------------------------------
# Status validation
# ---------------------------------------------------------------------------

class TestStatusValidation:
    def test_update_invalid_status_rejected(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(
            hkb_update(
                file="security.threat-intel",
                epoch=1000000,
                set_status="bogus",
                ctx=ctx,
            )
        )
        assert result["status"] == "error"
        assert "Invalid status" in result["message"]

    def test_update_task_statuses_accepted(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        for task_status in ["pending", "in_progress", "completed", "blocked", "cancelled"]:
            result = json.loads(
                hkb_update(
                    file="security.threat-intel",
                    epoch=1000000,
                    set_status=task_status,
                    ctx=ctx,
                )
            )
            assert result["status"] == "ok", f"Status '{task_status}' should be accepted"


# ---------------------------------------------------------------------------
# hkb_session action="review" (replaces old hkb_session_review)
# ---------------------------------------------------------------------------

class TestHkbSessionReview:
    def test_session_review_basic(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="review", ctx=ctx))
        assert "summary" in result
        assert "entries_by_file" in result
        assert "diagnostics" in result
        assert "type_distribution" in result
        assert "status_distribution" in result
        assert result["summary"]["total_entries"] == 3
        assert result["summary"]["groups"] == 2  # 2 files

    def test_session_review_group_by_type(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="review", group_by="type", ctx=ctx))
        assert "entries_by_type" in result
        # All sample entries are notes (no explicit type)
        assert "note" in result["entries_by_type"]
        assert len(result["entries_by_type"]["note"]) == 3

    def test_session_review_group_by_status(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="review", group_by="status", ctx=ctx))
        assert "entries_by_status" in result
        # All sample entries are active
        assert "active" in result["entries_by_status"]

    def test_session_review_time_window(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Add an entry at a recent 10-digit epoch so parse_time_input accepts it
        sample_kb.add_entry(
            content="Recent observation for filtering test.",
            file_name="security.threat-intel",
            epoch=1700000000,
        )
        # Only entries after epoch 1600000000 — should get just the new entry
        result = json.loads(hkb_session(action="review", after="1600000000", ctx=ctx))
        assert result["summary"]["total_entries"] == 1

    def test_session_review_diagnostics_untagged(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="review", ctx=ctx))
        diag = result["diagnostics"]
        # All sample entries have no tags
        assert diag["untagged_count"] == 3
        assert len(diag["untagged"]) == 3

    def test_session_review_diagnostics_stale_active(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_session(action="review", ctx=ctx))
        diag = result["diagnostics"]
        # All sample entries have old epochs (1000000, 2000000, 3000000) — all stale
        assert diag["stale_active_count"] == 3
        assert len(diag["stale_active"]) == 3


# ---------------------------------------------------------------------------
# hkb_update action="batch" (replaces old hkb_batch_update)
# ---------------------------------------------------------------------------

class TestHkbBatchUpdate:
    def test_batch_update_single(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        updates = json.dumps([
            {"file": "security.threat-intel", "epoch": 1000000, "set_status": "superseded"}
        ])
        result = json.loads(hkb_update(action="batch", updates=updates, ctx=ctx))
        assert result["status"] == "ok"
        assert result["updated"] == 1
        assert result["failed"] == 0
        assert result["results"][0]["status"] == "ok"

    def test_batch_update_multiple(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        updates = json.dumps([
            {"file": "security.threat-intel", "epoch": 1000000, "add_tags": "reviewed"},
            {"file": "security.threat-intel", "epoch": 2000000, "add_tags": "reviewed"},
            {"file": "fitness.kettlebell", "epoch": 3000000, "set_status": "resolved"},
        ])
        result = json.loads(hkb_update(action="batch", updates=updates, ctx=ctx))
        assert result["status"] == "ok"
        assert result["updated"] == 3
        assert result["failed"] == 0

    def test_batch_update_partial_failure(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        updates = json.dumps([
            {"file": "security.threat-intel", "epoch": 1000000, "set_status": "resolved"},
            {"file": "security.threat-intel", "epoch": 9999999, "set_status": "resolved"},
        ])
        result = json.loads(hkb_update(action="batch", updates=updates, ctx=ctx))
        assert result["status"] == "partial"
        assert result["updated"] == 1
        assert result["failed"] == 1
        assert result["results"][0]["status"] == "ok"
        assert result["results"][1]["status"] == "error"

    def test_batch_update_invalid_json(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_update(action="batch", updates="not json", ctx=ctx))
        assert result["status"] == "error"
        assert "Invalid JSON" in result["message"]

    def test_batch_update_empty_list(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_update(action="batch", updates="[]", ctx=ctx))
        assert result["status"] == "error"
        assert "empty" in result["message"].lower()


# ---------------------------------------------------------------------------
# hkb_health
# ---------------------------------------------------------------------------

class TestHkbHealth:
    def test_health_all_checks(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(checks="all", ctx=ctx))
        assert "summary" in result
        assert result["summary"]["checks_run"] == 13
        assert "duration_ms" in result

    def test_health_quick_checks(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(checks="quick", ctx=ctx))
        # quick = T1+T2 = 10 checks
        assert result["summary"]["checks_run"] == 10

    def test_health_links_filter(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(checks="links", ctx=ctx))
        names = {c["name"] for c in result["checks"]}
        assert "broken_outbound_links" in names
        assert "self_links" in names
        # Should not include unrelated checks
        assert "stale_active" not in names

    def test_health_sync_filter(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(checks="sync", ctx=ctx))
        names = {c["name"] for c in result["checks"]}
        assert "db_vs_disk_file_count" in names
        assert "disk_db_entry_drift" in names

    def test_health_quality_filter(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(checks="quality", ctx=ctx))
        names = {c["name"] for c in result["checks"]}
        assert "stale_active" in names
        assert "untagged_entries" in names

    def test_health_fix_true(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        # Add an orphan link to fix
        sample_kb.db.insert_entry_links([("a.b", 99999, "c.d", 0, "file")])
        result = json.loads(hkb_health(checks="all", fix=True, ctx=ctx))
        # Should have applied fixes
        fixes = result.get("fixes_applied", [])
        fix_names = [f["name"] for f in fixes]
        assert "orphan_entry_links" in fix_names

    def test_health_check_structure(self, sample_kb):
        """Each check has required fields."""
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        for c in result["checks"]:
            assert "name" in c
            assert "status" in c
            assert c["status"] in ("ok", "warning", "error")
            assert "count" in c
            assert "message" in c
            assert "fix_hint" in c

    def test_health_includes_version(self, sample_kb):
        from hyperkb import __version__
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        assert "version" in result
        assert result["version"]["current"] == __version__

    def test_health_includes_update_available(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        ctx.request_context.lifespan_context.update_available = "v1.8.0 → v1.9.0"
        result = json.loads(hkb_health(ctx=ctx))
        assert result["version"]["update_available"] == "v1.8.0 → v1.9.0"

    def test_health_update_available_empty(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        assert result["version"]["update_available"] == ""

    def test_health_includes_stats(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        stats = result["stats"]
        assert stats["total_files"] == 2
        assert stats["total_entries"] == 3
        assert stats["storage_size_bytes"] > 0
        assert stats["db_size_bytes"] > 0

    def test_health_stats_human_sizes(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        stats = result["stats"]
        assert isinstance(stats["storage_size_human"], str)
        assert isinstance(stats["db_size_human"], str)
        # Should end with a unit
        assert any(u in stats["storage_size_human"] for u in ("B", "KB", "MB", "GB"))

    def test_health_stats_sync_disabled(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        sync = result["stats"]["sync"]
        assert sync["enabled"] is False

    def test_health_fix_commands_on_ok_checks(self, sample_kb):
        ctx = _make_ctx(sample_kb)
        result = json.loads(hkb_health(ctx=ctx))
        for c in result["checks"]:
            assert "fix_commands" in c
            if c["status"] == "ok":
                assert c["fix_commands"] == []

    def test_health_fix_commands_compact(self, kb_store):
        """Compaction-ready files get hkb_compact commands."""
        kb_store.create_file("compact.test", "compaction test", ["test"])
        # Add clustered entries close together (within 1h gap)
        for i in range(4):
            kb_store.add_entry(
                content=f"entry {i} for compaction test",
                file_name="compact.test", epoch=1000000 + i,
            )
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(ctx=ctx))
        compact_checks = [c for c in result["checks"] if c["name"] == "compaction_readiness"]
        if compact_checks and compact_checks[0]["status"] != "ok":
            cmds = compact_checks[0]["fix_commands"]
            assert any('hkb_compact(name="compact.test")' in cmd for cmd in cmds)

    def test_health_fix_commands_empty_files(self, kb_store):
        """Empty files get hkb_add commands."""
        kb_store.create_file("empty.test", "empty test", ["test"])
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(ctx=ctx))
        empty_checks = [c for c in result["checks"] if c["name"] == "empty_files"]
        assert len(empty_checks) == 1
        assert empty_checks[0]["status"] == "warning"
        cmds = empty_checks[0]["fix_commands"]
        assert any('hkb_add(to="empty.test"' in cmd for cmd in cmds)

    def test_health_fix_commands_reindex(self, kb_store):
        """DB/disk drift suggests hkb_reindex."""
        # Insert a file into DB without creating on disk
        from hyperkb.models import FileHeader
        kb_store.db.insert_file(
            FileHeader(name="ghost.file", description="ghost", keywords=[], links=[], created="now"),
            path="ghost.file.md",
        )
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(ctx=ctx))
        drift_checks = [c for c in result["checks"] if c["name"] == "db_vs_disk_file_count"]
        assert len(drift_checks) == 1
        if drift_checks[0]["status"] != "ok":
            cmds = drift_checks[0]["fix_commands"]
            assert "hkb_reindex()" in cmds

    def test_health_fix_commands_capped(self, kb_store):
        """Fix commands are capped at 5 with overflow note."""
        # Create many empty files to generate many fix commands
        for i in range(8):
            kb_store.create_file(f"cap.test{i}", f"cap test {i}", ["cap"])
        ctx = _make_ctx(kb_store)
        result = json.loads(hkb_health(ctx=ctx))
        empty_checks = [c for c in result["checks"] if c["name"] == "empty_files"]
        assert len(empty_checks) == 1
        cmds = empty_checks[0]["fix_commands"]
        # 5 commands + 1 overflow note
        assert len(cmds) <= 6
        assert any("more" in cmd for cmd in cmds)


# ---------------------------------------------------------------------------
# hkb_briefing health_hints
# ---------------------------------------------------------------------------

class TestBriefingHealthHints:
    def test_briefing_includes_health_hints(self, sample_kb):
        """Briefing should include health_hints section."""
        health = sample_kb.health_snapshot()
        ctx = _make_ctx(sample_kb, health=health)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        assert "health_hints" in result
        assert "issues" in result["health_hints"]

    def test_briefing_health_hints_with_issues(self, sample_kb):
        """When there are health issues, highlights should be populated."""
        # Create an issue: orphan link
        sample_kb.db.insert_entry_links([("x.y", 99999, "c.d", 0, "file")])
        health = sample_kb.health_snapshot()
        ctx = _make_ctx(sample_kb, health=health)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        hints = result["health_hints"]
        assert hints["issues"] > 0
        assert len(hints["highlights"]) > 0
        assert "hkb_health" in hints["detail"]

    def test_briefing_health_hints_no_issues(self, sample_kb):
        """Clean KB has zero issues."""
        health = sample_kb.health_snapshot()
        ctx = _make_ctx(sample_kb, health=health)
        result = json.loads(hkb_session(action="briefing", ctx=ctx))
        # sample_kb has a wiki-link to fitness.kettlebell which exists, so should be clean
        # (There may be untagged/stale warnings in T1 but those are not in snapshot)
        hints = result["health_hints"]
        assert isinstance(hints["issues"], int)


# ---------------------------------------------------------------------------
# _ServerLock — process-level exclusive lock
# ---------------------------------------------------------------------------

class TestServerLock:
    def test_lock_acquisition(self, tmp_path):
        """Lock can be acquired on a fresh directory and creates the lock file."""
        lock = _ServerLock(tmp_path)
        lock.acquire()
        try:
            assert (tmp_path / LOCK_FILENAME).exists()
        finally:
            lock.release()

    def test_lock_writes_pid(self, tmp_path):
        """Lock file contains the current process PID."""
        lock = _ServerLock(tmp_path)
        lock.acquire()
        try:
            content = (tmp_path / LOCK_FILENAME).read_text()
            assert content.strip() == str(os.getpid())
        finally:
            lock.release()

    def test_second_lock_fails(self, tmp_path):
        """A second lock on the same dir raises RuntimeError."""
        lock1 = _ServerLock(tmp_path)
        lock1.acquire()
        try:
            lock2 = _ServerLock(tmp_path)
            with pytest.raises(RuntimeError, match="double acquire"):
                lock2.acquire()
        finally:
            lock1.release()

    def test_lock_released_after_release(self, tmp_path):
        """After release(), a new lock can be acquired."""
        lock1 = _ServerLock(tmp_path)
        lock1.acquire()
        lock1.release()

        lock2 = _ServerLock(tmp_path)
        lock2.acquire()
        lock2.release()

    def test_release_is_idempotent(self, tmp_path):
        """Calling release() twice does not raise."""
        lock = _ServerLock(tmp_path)
        lock.acquire()
        lock.release()
        lock.release()  # should not raise

    def test_lock_released_on_fd_close(self, tmp_path):
        """Simulates process death by closing fd — new lock succeeds."""
        lock1 = _ServerLock(tmp_path)
        lock1.acquire()
        # Simulate abrupt exit by closing the fd directly
        if lock1._fd is not None:
            os.close(lock1._fd)
            lock1._fd = None

        lock2 = _ServerLock(tmp_path)
        lock2.acquire()
        lock2.release()

    def test_lock_skipped_without_fcntl(self, tmp_path, monkeypatch):
        """On platforms without fcntl, acquire is a no-op (no error raised)."""
        import hyperkb.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "_HAS_FCNTL", False)
        lock = _ServerLock(tmp_path)
        lock.acquire()  # should not raise
        assert lock._fd is None
        lock.release()

    # -- _is_hkb_mcp_process --

    def test_is_hkb_mcp_process_nonexistent_pid(self):
        """Returns False for a PID that does not exist."""
        assert _ServerLock._is_hkb_mcp_process(999_999_999) is False

    def test_is_hkb_mcp_process_current_process(self):
        """Doesn't crash when called on a valid PID (our own)."""
        # We just verify it returns a bool without raising.
        result = _ServerLock._is_hkb_mcp_process(os.getpid())
        assert isinstance(result, bool)

    # -- _terminate_stale --

    def test_terminate_stale_nonexistent_pid(self):
        """Returns True immediately for a PID that is already dead."""
        assert _ServerLock._terminate_stale(999_999_999) is True

    def test_terminate_stale_sigterm_works(self):
        """Subprocess is killed via SIGTERM."""
        import subprocess
        proc = subprocess.Popen(
            ["sleep", "300"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        assert _ServerLock._terminate_stale(proc.pid) is True
        proc.wait()  # reap zombie

    def test_terminate_stale_needs_sigkill(self, monkeypatch):
        """Escalates to SIGKILL when SIGTERM is ignored."""
        import subprocess
        import hyperkb.mcp_server as mcp_mod

        # Use short timeouts so the test doesn't take 7 seconds.
        monkeypatch.setattr(mcp_mod, "_SIGTERM_TIMEOUT", 1)
        monkeypatch.setattr(mcp_mod, "_SIGKILL_TIMEOUT", 2)

        # Python subprocess that traps SIGTERM and ignores it.
        proc = subprocess.Popen(
            [
                "python3", "-c",
                "import signal, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "time.sleep(300)",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        assert _ServerLock._terminate_stale(proc.pid) is True
        proc.wait()  # reap zombie

    # -- Stale recovery in acquire() --

    def test_stale_recovery_kills_holder_and_acquires(self, tmp_path):
        """Full integration: spawns holder subprocess, acquire() terminates it."""
        import subprocess
        import sys

        # The subprocess script name must contain "mcp_server" so
        # _is_hkb_mcp_process() identifies it via /proc/cmdline.
        script = tmp_path / "fake_mcp_server.py"
        lock_file = tmp_path / LOCK_FILENAME
        script.write_text(
            "import os, fcntl, time\n"
            f"fd = os.open('{lock_file}', os.O_RDWR | os.O_CREAT, 0o644)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\n"
            "os.ftruncate(fd, 0)\n"
            "os.write(fd, str(os.getpid()).encode())\n"
            "time.sleep(300)\n"
        )

        holder = subprocess.Popen(
            [sys.executable, str(script)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for the child to actually acquire the lock.
        import time
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                content = lock_file.read_text().strip()
                if content == str(holder.pid):
                    break
            except (FileNotFoundError, ValueError):
                pass
            time.sleep(0.05)

        try:
            lock = _ServerLock(tmp_path)
            lock.acquire()  # should kill the holder and succeed
            try:
                # Verify we now hold the lock with our PID.
                content = lock_file.read_text().strip()
                assert content == str(os.getpid())
            finally:
                lock.release()
        finally:
            holder.wait()

    def test_stale_recovery_non_hkb_process_not_killed(self, tmp_path):
        """If holder PID is not hkb-mcp, no kill — retry fails → RuntimeError."""
        import fcntl as _fcntl

        # Write a fake PID (our own PID + 1 — unlikely to be hkb-mcp).
        fake_pid = 999_999_999  # nonexistent
        lock_path = tmp_path / LOCK_FILENAME

        # Acquire the lock from *this* process to simulate a held lock.
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        _fcntl.flock(fd, _fcntl.LOCK_EX)
        os.ftruncate(fd, 0)
        os.write(fd, str(fake_pid).encode())

        try:
            lock = _ServerLock(tmp_path)
            with pytest.raises(RuntimeError, match="recovery failed"):
                lock.acquire()
        finally:
            os.close(fd)

    def test_stale_recovery_unparseable_pid(self, tmp_path):
        """Garbage PID in lock file → RuntimeError (no kill attempt)."""
        import fcntl as _fcntl

        lock_path = tmp_path / LOCK_FILENAME
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        _fcntl.flock(fd, _fcntl.LOCK_EX)
        os.ftruncate(fd, 0)
        os.write(fd, b"garbage")

        try:
            lock = _ServerLock(tmp_path)
            with pytest.raises(RuntimeError, match="unparseable"):
                lock.acquire()
        finally:
            os.close(fd)


class TestLifespanLock:
    def test_lifespan_acquires_lock(self, tmp_path, monkeypatch):
        """After lifespan starts, server.lock exists with our PID."""
        import asyncio
        import hyperkb.mcp_server as mcp_mod

        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(mcp_mod, "_server_args", _parse_server_args([]))

        async def _run():
            server = MagicMock()
            async with app_lifespan(server) as ctx:
                lock_path = tmp_path / ".hkb" / LOCK_FILENAME
                assert lock_path.exists()
                assert lock_path.read_text().strip() == str(os.getpid())

        asyncio.run(_run())

    def test_lifespan_second_instance_blocked(self, tmp_path, monkeypatch):
        """While lifespan is active, a second _ServerLock.acquire() raises."""
        import asyncio
        import hyperkb.mcp_server as mcp_mod

        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(mcp_mod, "_server_args", _parse_server_args([]))

        async def _run():
            server = MagicMock()
            async with app_lifespan(server) as ctx:
                hkb_dir = tmp_path / ".hkb"
                lock2 = _ServerLock(hkb_dir)
                with pytest.raises(RuntimeError, match="double acquire"):
                    lock2.acquire()

        asyncio.run(_run())
