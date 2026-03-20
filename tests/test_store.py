"""Tests for hyperkb.store — integration: create→add→search→show→reindex→check."""

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch
import pytest

from hyperkb.db import TimeoutLock
from hyperkb.store import KnowledgeStore


class TestInit:
    def test_init_creates_hkb_dir(self, kb_config, kb_root):
        store = KnowledgeStore(kb_config)
        result = store.init()
        assert "initialized" in result.lower()
        assert (kb_root / ".hkb" / "config.json").exists()
        assert (kb_root / ".hkb" / "index.db").exists()
        assert (kb_root / ".hkb" / "storage").is_dir()
        store.close()

    def test_store_write_lock_is_timeout_lock(self, kb_store):
        """Store write lock must be a TimeoutLock, not a bare threading.Lock."""
        assert isinstance(kb_store._write_lock, TimeoutLock)
        assert kb_store._write_lock._timeout == 60


class TestCreateFile:
    def test_create_file(self, kb_store, kb_root):
        result = kb_store.create_file(
            name="test.file",
            description="Test file for testing",
            keywords=["test", "file"],
        )
        assert "Created" in result
        assert (kb_root / ".hkb" / "storage" / "test.file.md").exists()

    def test_create_duplicate_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with pytest.raises(ValueError, match="already exists"):
            kb_store.create_file(name="test.file", description="d", keywords=[])

    def test_create_invalid_name_raises(self, kb_store):
        with pytest.raises(ValueError, match="Invalid filename"):
            kb_store.create_file(name="single", description="d", keywords=[])

    def test_create_with_links(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(
            name="c.d", description="d", keywords=[], links=["a.b"]
        )
        links = kb_store.get_links("c.d")
        assert "a.b" in links["outbound"]


class TestAddEntry:
    def test_add_entry_direct(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        result = kb_store.add_entry("test content", file_name="test.file", epoch=12345)
        assert "Added to test.file" in result
        assert "12345" in result

    def test_add_entry_nonexistent_file_raises(self, kb_store):
        with pytest.raises(ValueError, match="not found"):
            kb_store.add_entry("content", file_name="no.such.file")

    def test_add_entry_strips_md_extension(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        result = kb_store.add_entry("content", file_name="test.file.md", epoch=100)
        assert "Added to test.file" in result

    def test_add_entry_no_match(self, kb_store):
        # No files exist, auto-routing should return NO_MATCH
        result = kb_store.add_entry("completely random content about nothing")
        assert "NO_MATCH" in result

    def test_add_entry_warns_broken_links(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        result = kb_store.add_entry(
            "See [[nonexistent.file]]",
            file_name="test.file",
            epoch=100,
        )
        assert "broken links" in result.lower()

    def test_oversized_entry_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.config.max_entry_size = 100  # 100 bytes
        big_content = "x" * 200
        with pytest.raises(ValueError, match="Entry too large"):
            kb_store.add_entry(big_content, file_name="test.file")

    def test_normal_entry_under_limit(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.config.max_entry_size = 1_048_576
        result = kb_store.add_entry("normal content", file_name="test.file", epoch=100)
        assert "Added to test.file" in result

    def test_epoch_collision_handled(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        r1 = kb_store.add_entry("first", file_name="test.file", epoch=100)
        r2 = kb_store.add_entry("second", file_name="test.file", epoch=100)
        # Both should succeed with different epochs
        assert "100" in r1
        assert "101" in r2

    def test_add_entry_empty_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with pytest.raises(ValueError, match="empty or whitespace"):
            kb_store.add_entry("", file_name="test.file")

    def test_add_entry_whitespace_only_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with pytest.raises(ValueError, match="empty or whitespace"):
            kb_store.add_entry("   \n\t  ", file_name="test.file")

    def test_add_entry_delimiter_marker_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with pytest.raises(ValueError, match="entry delimiters"):
            kb_store.add_entry("some text\n>>> 9999999999\nmore text", file_name="test.file")

    def test_add_entry_end_delimiter_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with pytest.raises(ValueError, match="entry delimiters"):
            kb_store.add_entry("some text\n<<<\nmore text", file_name="test.file")

    def test_add_entry_inline_delimiter_accepted(self, kb_store):
        """Delimiter-like text mid-line should NOT be rejected."""
        kb_store.create_file(name="test.file", description="d", keywords=[])
        result = kb_store.add_entry(
            "The format uses >>> 1234567890 as markers",
            file_name="test.file", epoch=100,
        )
        assert "Added to test.file" in result

    def test_append_entry_rollback_on_file_write_failure(self, kb_store):
        """DB entry should be rolled back if file write fails."""
        kb_store.create_file(name="test.file", description="d", keywords=[])

        with patch("hyperkb.store.append_entry_to_file", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                kb_store.add_entry("should be rolled back", file_name="test.file", epoch=500)

        # DB should NOT contain the entry
        entries = kb_store.db.get_entries("test.file")
        assert len(entries) == 0

        # File should not contain the entry either
        data = kb_store.show_file("test.file")
        assert len(data["entries"]) == 0


class TestShowFile:
    def test_show_file(self, sample_kb):
        data = sample_kb.show_file("security.threat-intel")
        assert data["header"]["name"] == "security.threat-intel"
        assert len(data["entries"]) == 2

    def test_show_file_with_md(self, sample_kb):
        data = sample_kb.show_file("security.threat-intel.md")
        assert data["header"]["name"] == "security.threat-intel"

    def test_show_file_not_found(self, sample_kb):
        with pytest.raises(FileNotFoundError):
            sample_kb.show_file("no.such.file")

    def test_show_file_after_epoch(self, sample_kb):
        data = sample_kb.show_file("security.threat-intel", after_epoch=1500000)
        assert len(data["entries"]) == 1
        assert data["entries"][0]["epoch"] == 2000000

    def test_show_file_before_epoch(self, sample_kb):
        data = sample_kb.show_file("security.threat-intel", before_epoch=1500000)
        assert len(data["entries"]) == 1
        assert data["entries"][0]["epoch"] == 1000000

    def test_show_file_last_n(self, sample_kb):
        data = sample_kb.show_file("security.threat-intel", last_n=1)
        assert len(data["entries"]) == 1
        assert data["entries"][0]["epoch"] == 2000000

    def test_show_file_after_epoch_zero(self, sample_kb):
        """Epoch 0 edge case: after_epoch=0 should include all entries."""
        data = sample_kb.show_file("security.threat-intel", after_epoch=0)
        assert len(data["entries"]) == 2


class TestListFiles:
    def test_list_files(self, sample_kb):
        files = sample_kb.list_files()
        assert len(files) == 2

    def test_list_files_with_domain(self, sample_kb):
        files = sample_kb.list_files(domain="security")
        assert len(files) == 1
        assert files[0]["name"] == "security.threat-intel"


class TestLinks:
    def test_get_links(self, sample_kb):
        links = sample_kb.get_links("fitness.kettlebell")
        # security.threat-intel has a wikilink to fitness.kettlebell
        assert "security.threat-intel" in links["inbound_entries"]


class TestCheckContent:
    def test_check_returns_candidates(self, sample_kb):
        candidates = sample_kb.check_content("threat intelligence IOC feeds")
        assert len(candidates) >= 1
        assert candidates[0].name == "security.threat-intel"

    def test_check_empty_kb(self, kb_store):
        candidates = kb_store.check_content("anything")
        assert candidates == []


class TestReindex:
    def test_reindex(self, sample_kb):
        result = sample_kb.reindex()
        assert "2 files" in result
        assert "3 entries" in result

    def test_reindex_restores_search(self, sample_kb):
        sample_kb.reindex()
        results = sample_kb.search("AlienVault", mode="bm25")
        assert len(results) >= 1

    def test_reindex_skips_corrupt_file(self, kb_store):
        """Reindex should skip corrupt files and still index good ones."""
        kb_store.create_file(name="good.file", description="Good file", keywords=[])
        kb_store.add_entry("good content here", file_name="good.file", epoch=100)
        # Write a binary-corrupt file that will fail UTF-8 decoding
        corrupt = kb_store.storage_dir / "bad.file.md"
        corrupt.write_bytes(b"\x80\x81\x82\xff\xfe invalid binary")
        result = kb_store.reindex()
        # Good file should still be indexed
        assert "1 files" in result
        assert "Skipped 1 corrupt" in result
        # Good file entries should be searchable
        results = kb_store.search("good content", mode="bm25")
        assert len(results) >= 1

    def test_reindex_populates_entry_links(self, sample_kb):
        """Wiki-links in sample_kb entries produce entry_links rows after reindex."""
        # sample_kb has [[fitness.kettlebell]] in security.threat-intel entry at epoch 2000000
        sample_kb.reindex()
        refs = sample_kb.db.get_entry_references("security.threat-intel", 2000000)
        assert len(refs) >= 1
        target_files = [r["target_file"] for r in refs]
        assert "fitness.kettlebell" in target_files

    def test_reindex_clears_stale_links(self, sample_kb):
        """Second reindex doesn't double the links."""
        sample_kb.reindex()
        refs1 = sample_kb.db.get_entry_references("security.threat-intel", 2000000)
        sample_kb.reindex()
        refs2 = sample_kb.db.get_entry_references("security.threat-intel", 2000000)
        assert len(refs1) == len(refs2)

    def test_reindex_entry_level_links(self, kb_store):
        """[[file#epoch]] produces link_type='entry' with correct target_epoch."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("content", file_name="c.d", epoch=500)
        kb_store.add_entry("See [[c.d#500]] for details", file_name="a.b", epoch=100)
        kb_store.reindex()
        refs = kb_store.db.get_entry_references("a.b", 100)
        entry_refs = [r for r in refs if r["link_type"] == "entry"]
        assert len(entry_refs) == 1
        assert entry_refs[0]["target_file"] == "c.d"
        assert entry_refs[0]["target_epoch"] == 500

    def test_reindex_latest_links(self, kb_store):
        """[[file#latest]] produces link_type='latest' with target_epoch=-1."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d#latest]]", file_name="a.b", epoch=100)
        kb_store.reindex()
        refs = kb_store.db.get_entry_references("a.b", 100)
        latest_refs = [r for r in refs if r["link_type"] == "latest"]
        assert len(latest_refs) == 1
        assert latest_refs[0]["target_file"] == "c.d"
        assert latest_refs[0]["target_epoch"] == -1


class TestSearch:
    def test_search_hybrid(self, sample_kb):
        results = sample_kb.search("AlienVault")
        assert len(results) >= 1

    def test_search_with_time_filter(self, sample_kb):
        # Only entries after epoch 1500000 (should exclude epoch 1000000)
        results = sample_kb.search("AlienVault", mode="bm25", after_epoch=1500000)
        for r in results:
            assert r.epoch > 1500000

    def test_search_with_before_filter(self, sample_kb):
        results = sample_kb.search("content", mode="bm25", before_epoch=1500000)
        for r in results:
            assert r.epoch < 1500000


class TestConcurrency:
    def test_concurrent_add_entries(self, kb_store):
        """10 concurrent add_entry calls should all succeed without data corruption."""
        kb_store.create_file(name="test.concurrent", description="Concurrency test", keywords=[])

        def add_one(i):
            return kb_store.add_entry(
                f"concurrent entry number {i}",
                file_name="test.concurrent",
                epoch=5000 + i,
            )

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(add_one, i) for i in range(10)]
            results = [f.result() for f in as_completed(futures)]

        # All should succeed
        assert len(results) == 10
        for r in results:
            assert "Added to test.concurrent" in r

        # All entries should be present in the file
        data = kb_store.show_file("test.concurrent")
        assert len(data["entries"]) == 10


class TestRoundtrip:
    def test_create_add_search_show_roundtrip(self, kb_store):
        """End-to-end: create file, add entry, search finds it, show returns it."""
        kb_store.create_file(
            name="test.roundtrip",
            description="Integration test file",
            keywords=["integration"],
        )
        kb_store.add_entry(
            "unique-canary-phrase for roundtrip",
            file_name="test.roundtrip",
        )
        results = kb_store.search("unique-canary-phrase", mode="bm25")
        assert len(results) >= 1
        assert results[0].file_name == "test.roundtrip"
        data = kb_store.show_file("test.roundtrip")
        assert any("unique-canary-phrase" in e["content"] for e in data["entries"])


class TestMetadataInAddEntry:
    def test_add_entry_with_metadata(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        result = kb_store.add_entry(
            "@type: decision\n@status: active\n@tags: arch, db\nA decision about architecture.",
            file_name="test.file",
            epoch=100,
        )
        assert "Added to test.file" in result
        # DB should have metadata columns populated
        entry = kb_store.db.get_entry("test.file", 100)
        assert entry["status"] == "active"
        assert entry["entry_type"] == "decision"
        assert entry["tags"] == "arch, db"
        # Content in DB should be prose only (metadata stripped)
        assert entry["content"] == "A decision about architecture."

    def test_add_entry_without_metadata_uses_defaults(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry("Plain content.", file_name="test.file", epoch=200)
        entry = kb_store.db.get_entry("test.file", 200)
        assert entry["status"] == "active"
        assert entry["entry_type"] == "note"
        assert entry["tags"] == ""

    def test_search_with_status_filter(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry(
            "@status: active\nActive canary entry.",
            file_name="test.file", epoch=100,
        )
        kb_store.add_entry(
            "@status: superseded\nSuperseded canary entry.",
            file_name="test.file", epoch=200,
        )
        results = kb_store.search("canary", mode="bm25", status="active")
        assert all("Active" in r.content for r in results)

    def test_metadata_roundtrip_via_file(self, kb_store):
        """Metadata should be stored in file and survive reindex."""
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry(
            "@type: finding\n@tags: bug, fix\nA finding about a bug.",
            file_name="test.file", epoch=300,
        )
        # Reindex from disk
        kb_store.reindex()
        entry = kb_store.db.get_entry("test.file", 300)
        assert entry["entry_type"] == "finding"
        assert entry["tags"] == "bug, fix"
        assert entry["content"] == "A finding about a bug."


class TestSkillEntryRoundtrip:
    def test_skill_entry_add_and_search(self, kb_store):
        """Create file, add skill entry, search by type='skill', verify it returns."""
        kb_store.create_file(name="devops.ssh", description="SSH tips", keywords=["ssh"])
        kb_store.add_entry(
            "@type: skill\n@tags: ssh, tunneling\n\n"
            "**Problem:** SSH drops after idle. "
            "**Solution:** Add ServerAliveInterval 60. "
            "**Context:** OpenSSH 9.x behind Squid.",
            file_name="devops.ssh",
            epoch=100,
        )
        entry = kb_store.db.get_entry("devops.ssh", 100)
        assert entry["entry_type"] == "skill"
        results = kb_store.search("SSH idle", mode="bm25", entry_type="skill")
        assert len(results) >= 1
        assert any(r.file_name == "devops.ssh" for r in results)


class TestUpdateEntry:
    def test_update_content_only(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry(
            "@type: finding\nOriginal content.",
            file_name="test.file", epoch=100,
        )
        result = kb_store.update_entry("test.file", 100, new_content="Updated content.")
        assert result["status"] == "ok"
        # DB should have updated content
        entry = kb_store.db.get_entry("test.file", 100)
        assert entry["content"] == "Updated content."
        # Metadata should be preserved
        assert entry["entry_type"] == "finding"
        # File on disk should match
        data = kb_store.show_file("test.file")
        assert data["entries"][0]["content"] == "Updated content."

    def test_update_status_only(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry("Original.", file_name="test.file", epoch=200)
        result = kb_store.update_entry("test.file", 200, set_status="superseded")
        assert result["status"] == "ok"
        entry = kb_store.db.get_entry("test.file", 200)
        assert entry["status"] == "superseded"
        assert entry["content"] == "Original."

    def test_update_add_tags(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry(
            "@tags: existing\nContent.",
            file_name="test.file", epoch=300,
        )
        kb_store.update_entry("test.file", 300, add_tags="new, another")
        entry = kb_store.db.get_entry("test.file", 300)
        tags = {t.strip() for t in entry["tags"].split(",")}
        assert "existing" in tags
        assert "new" in tags
        assert "another" in tags

    def test_update_remove_tags(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry(
            "@tags: a, b, c\nContent.",
            file_name="test.file", epoch=400,
        )
        kb_store.update_entry("test.file", 400, remove_tags="b")
        entry = kb_store.db.get_entry("test.file", 400)
        tags = {t.strip() for t in entry["tags"].split(",") if t.strip()}
        assert "a" in tags
        assert "c" in tags
        assert "b" not in tags

    def test_update_nonexistent_entry_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        with pytest.raises(ValueError, match="Entry not found"):
            kb_store.update_entry("test.file", 9999, set_status="resolved")

    def test_update_nonexistent_file_raises(self, kb_store):
        with pytest.raises(ValueError, match="Entry not found"):
            kb_store.update_entry("no.such.file", 100, set_status="resolved")

    def test_update_no_changes_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry("Content.", file_name="test.file", epoch=500)
        with pytest.raises(ValueError, match="At least one"):
            kb_store.update_entry("test.file", 500)

    def test_update_reflects_in_search(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry("flamingo original.", file_name="test.file", epoch=600)
        kb_store.update_entry("test.file", 600, new_content="penguin updated.")
        # New term should be searchable
        results = kb_store.search("penguin", mode="bm25")
        assert len(results) >= 1
        # Old term should be gone
        results = kb_store.search("flamingo", mode="bm25")
        assert len(results) == 0

    def test_update_file_on_disk_matches(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry(
            "@status: active\nOriginal prose.",
            file_name="test.file", epoch=700,
        )
        kb_store.update_entry("test.file", 700, set_status="resolved", new_content="Resolved prose.")
        # Read from disk
        data = kb_store.show_file("test.file")
        assert len(data["entries"]) == 1
        assert data["entries"][0]["content"] == "Resolved prose."


class TestArchiveEntry:
    def test_archive_moves_entry(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=["test"])
        kb_store.add_entry("Entry to archive.", file_name="test.file", epoch=100)
        kb_store.add_entry("Entry to keep.", file_name="test.file", epoch=200)
        result = kb_store.archive_entry("test.file", 100)
        assert result["status"] == "ok"
        assert result["archived_to"] == "test.file.archive"
        # Source file should only have the kept entry
        data = kb_store.show_file("test.file")
        assert len(data["entries"]) == 1
        assert data["entries"][0]["epoch"] == 200
        # Archive file should have the archived entry
        data = kb_store.show_file("test.file.archive")
        assert len(data["entries"]) == 1
        assert data["entries"][0]["epoch"] == 100

    def test_archive_creates_archive_file(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=["test"])
        kb_store.add_entry("To archive.", file_name="test.file", epoch=100)
        archive_path = kb_store.storage_dir / "test.file.archive.md"
        assert not archive_path.exists()
        kb_store.archive_entry("test.file", 100)
        assert archive_path.exists()

    def test_archive_excludes_from_search(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=["test"])
        kb_store.add_entry("unique-archive-canary content.", file_name="test.file", epoch=100)
        kb_store.archive_entry("test.file", 100)
        # Default search should not find it
        results = kb_store.search("unique-archive-canary", mode="bm25")
        assert len(results) == 0
        # Include-archived should find it
        results = kb_store.search("unique-archive-canary", mode="bm25", include_archived=True)
        assert len(results) >= 1

    def test_archive_nonexistent_file_raises(self, kb_store):
        with pytest.raises(ValueError, match="not found"):
            kb_store.archive_entry("no.such.file", 100)

    def test_archive_nonexistent_epoch_raises(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=[])
        kb_store.add_entry("Content.", file_name="test.file", epoch=100)
        with pytest.raises(ValueError, match="not found"):
            kb_store.archive_entry("test.file", 9999)

    def test_archive_reuses_existing_archive_file(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=["test"])
        kb_store.add_entry("First.", file_name="test.file", epoch=100)
        kb_store.add_entry("Second.", file_name="test.file", epoch=200)
        kb_store.archive_entry("test.file", 100)
        kb_store.archive_entry("test.file", 200)
        data = kb_store.show_file("test.file.archive")
        assert len(data["entries"]) == 2

    def test_archive_file_shown_in_list(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=["test"])
        kb_store.add_entry("To archive.", file_name="test.file", epoch=100)
        kb_store.archive_entry("test.file", 100)
        files = kb_store.list_files()
        names = [f["name"] for f in files]
        assert "test.file.archive" in names

    def test_reindex_handles_archive_files(self, kb_store):
        kb_store.create_file(name="test.file", description="d", keywords=["test"])
        kb_store.add_entry("To archive.", file_name="test.file", epoch=100)
        kb_store.archive_entry("test.file", 100)
        result = kb_store.reindex()
        assert "2 files" in result  # source + archive


class TestCompactFile:
    """Tests for KnowledgeStore.compact_file()."""

    def _make_file_with_entries(self, kb_store, name, epochs, gap=100):
        """Helper: create file and add entries at given epochs."""
        kb_store.create_file(name=name, description="test file", keywords=["test"])
        for i, ep in enumerate(epochs):
            kb_store.add_entry(f"Entry {i} content.", file_name=name, epoch=ep)

    def test_compact_cluster_detection(self, kb_store):
        """Entries within gap are grouped; entries across gap are split."""
        # Two clusters: [1000,1100,1200] and [20000,20100]
        self._make_file_with_entries(
            kb_store, "test.cluster",
            [1000, 1100, 1200, 20000, 20100],
        )
        result = kb_store.compact_file(
            "test.cluster", gap_seconds=5000, min_cluster=2, min_age_seconds=0,
            dry_run=True,
        )
        assert result["status"] == "preview"
        assert len(result["clusters"]) == 2
        assert result["clusters"][0]["size"] == 3
        assert result["clusters"][1]["size"] == 2

    def test_compact_single_entry_not_clustered(self, kb_store):
        """Isolated entries form size-1 clusters (never eligible)."""
        # Three entries far apart
        self._make_file_with_entries(
            kb_store, "test.singles",
            [1000, 100000, 200000],
        )
        result = kb_store.compact_file(
            "test.singles", gap_seconds=5000, min_cluster=2, min_age_seconds=0,
            dry_run=True,
        )
        assert result["total_entries"] == 3
        assert all(c["size"] == 1 for c in result["clusters"])
        assert result["eligible_clusters"] == 0

    def test_compact_eligibility_min_cluster(self, kb_store):
        """Clusters below min_cluster are not eligible."""
        # One cluster of 2, one of 4
        self._make_file_with_entries(
            kb_store, "test.mincluster",
            [1000, 1100, 50000, 50100, 50200, 50300],
        )
        result = kb_store.compact_file(
            "test.mincluster", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=True,
        )
        # Cluster of 2 not eligible, cluster of 4 is eligible
        eligible = [c for c in result["clusters"] if c["eligible"]]
        assert len(eligible) == 1
        assert eligible[0]["size"] == 4

    def test_compact_eligibility_min_age(self, kb_store):
        """Recent clusters are not eligible."""
        import time
        now = int(time.time())
        # Old cluster (eligible) and recent cluster (not eligible)
        self._make_file_with_entries(
            kb_store, "test.minage",
            [1000, 1100, 1200, now - 10, now - 5, now],
        )
        result = kb_store.compact_file(
            "test.minage", gap_seconds=5000, min_cluster=3,
            min_age_seconds=3600,  # 1 hour
            dry_run=True,
        )
        eligible = [c for c in result["clusters"] if c["eligible"]]
        assert len(eligible) == 1
        assert eligible[0]["epochs"][0] == 1000

    def test_compact_dry_run_no_side_effects(self, kb_store):
        """Preview mode does not modify files or DB."""
        self._make_file_with_entries(
            kb_store, "test.dryrun",
            [1000, 1100, 1200],
        )
        original_data = kb_store.show_file("test.dryrun")
        result = kb_store.compact_file(
            "test.dryrun", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=True,
        )
        assert result["status"] == "preview"
        assert result["eligible_clusters"] == 1
        # File unchanged
        after_data = kb_store.show_file("test.dryrun")
        assert len(after_data["entries"]) == len(original_data["entries"])
        # No archive file created
        archive_path = kb_store.storage_dir / "test.dryrun.archive.md"
        assert not archive_path.exists()

    def test_compact_executes_archive(self, kb_store):
        """Execution moves originals to .archive file."""
        self._make_file_with_entries(
            kb_store, "test.exec",
            [1000, 1100, 1200],
        )
        result = kb_store.compact_file(
            "test.exec", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result["status"] == "ok"
        assert result["entries_archived"] == 3
        # Archive file should have the 3 original entries
        archive_data = kb_store.show_file("test.exec.archive")
        assert len(archive_data["entries"]) == 3

    def test_compact_summary_entry_created(self, kb_store):
        """Summary entry replaces cluster in main file with concatenated content."""
        self._make_file_with_entries(
            kb_store, "test.summary",
            [1000, 1100, 1200],
        )
        kb_store.compact_file(
            "test.summary", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        data = kb_store.show_file("test.summary")
        assert len(data["entries"]) == 1
        summary = data["entries"][0]
        assert "[Compacted 3 entries" in summary["content"]
        assert "Entry 0 content." in summary["content"]
        assert "Entry 1 content." in summary["content"]
        assert "Entry 2 content." in summary["content"]
        assert "[archived:" in summary["content"]

    def test_compact_summary_preserves_tags(self, kb_store):
        """Summary entry has union of tags from all cluster entries."""
        kb_store.create_file(name="test.tags", description="d", keywords=["test"])
        kb_store.add_entry("@tags: alpha, beta\nFirst.", file_name="test.tags", epoch=1000)
        kb_store.add_entry("@tags: beta, gamma\nSecond.", file_name="test.tags", epoch=1100)
        kb_store.add_entry("Third.", file_name="test.tags", epoch=1200)

        kb_store.compact_file(
            "test.tags", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        data = kb_store.show_file("test.tags")
        assert len(data["entries"]) == 1
        # Search DB for the summary entry's tags
        db_entry = kb_store.db.get_entry("test.tags", 1000)
        tags = {t.strip() for t in db_entry["tags"].split(",") if t.strip()}
        assert tags == {"alpha", "beta", "gamma"}

    def test_compact_summary_epoch(self, kb_store):
        """Summary entry uses the first entry's epoch."""
        self._make_file_with_entries(
            kb_store, "test.epoch",
            [5000, 5100, 5200],
        )
        kb_store.compact_file(
            "test.epoch", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        data = kb_store.show_file("test.epoch")
        assert len(data["entries"]) == 1
        assert data["entries"][0]["epoch"] == 5000

    def test_compact_updates_header_compacted(self, kb_store):
        """FileHeader.compacted is set to current ISO timestamp after compaction."""
        self._make_file_with_entries(
            kb_store, "test.header",
            [1000, 1100, 1200],
        )
        kb_store.compact_file(
            "test.header", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        from hyperkb.format import parse_file
        header, _ = parse_file(kb_store.storage_dir / "test.header.md")
        assert header.compacted != ""
        assert "T" in header.compacted  # ISO format

    def test_compact_skips_already_archived(self, kb_store):
        """Entries with @status:archived/superseded/resolved are not clustered."""
        kb_store.create_file(name="test.skip", description="d", keywords=["test"])
        kb_store.add_entry("Active 1.", file_name="test.skip", epoch=1000)
        kb_store.add_entry("@status: superseded\nOld.", file_name="test.skip", epoch=1100)
        kb_store.add_entry("Active 2.", file_name="test.skip", epoch=1200)

        result = kb_store.compact_file(
            "test.skip", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=True,
        )
        # Only 2 active entries clustered, so no eligible cluster at min_cluster=3
        assert result["eligible_clusters"] == 0

    def test_compact_empty_file(self, kb_store):
        """Empty file returns no-op result."""
        kb_store.create_file(name="test.empty", description="d", keywords=["test"])
        result = kb_store.compact_file(
            "test.empty", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=True,
        )
        assert result["total_entries"] == 0
        assert result["clusters"] == []

    def test_compact_no_eligible_clusters(self, kb_store):
        """All clusters too small → clean result with no changes."""
        self._make_file_with_entries(
            kb_store, "test.noelig",
            [1000, 1100],  # cluster of 2, below min_cluster=3
        )
        result = kb_store.compact_file(
            "test.noelig", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result["status"] == "ok"
        assert result["compacted_clusters"] == 0
        assert result["entries_archived"] == 0
        # Original entries still present
        data = kb_store.show_file("test.noelig")
        assert len(data["entries"]) == 2

    def test_compact_file_not_found(self, kb_store):
        """Compacting non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            kb_store.compact_file("no.such.file")

    def test_compact_preserves_non_cluster_entries(self, kb_store):
        """Entries outside eligible clusters are not affected."""
        import time
        now = int(time.time())
        # Old cluster [1000,1100,1200] + recent entry [now]
        kb_store.create_file(name="test.mixed", description="d", keywords=["test"])
        kb_store.add_entry("Old 1.", file_name="test.mixed", epoch=1000)
        kb_store.add_entry("Old 2.", file_name="test.mixed", epoch=1100)
        kb_store.add_entry("Old 3.", file_name="test.mixed", epoch=1200)
        kb_store.add_entry("Recent.", file_name="test.mixed", epoch=now)

        result = kb_store.compact_file(
            "test.mixed", gap_seconds=5000, min_cluster=3, min_age_seconds=3600,
            dry_run=False,
        )
        assert result["compacted_clusters"] == 1
        assert result["entries_archived"] == 3
        assert result["entries_remaining"] == 2  # 1 summary + 1 recent
        data = kb_store.show_file("test.mixed")
        assert len(data["entries"]) == 2
        # Recent entry preserved
        recent_entry = [e for e in data["entries"] if e["epoch"] == now]
        assert len(recent_entry) == 1

    # --- Stress / integration tests ---

    def test_compact_search_finds_summary_content(self, kb_store):
        """After compaction, search still finds terms from original entries."""
        kb_store.create_file(name="test.search", description="d", keywords=["test"])
        kb_store.add_entry("The flamingo algorithm is novel.", file_name="test.search", epoch=1000)
        kb_store.add_entry("Flamingo uses spectral analysis.", file_name="test.search", epoch=1100)
        kb_store.add_entry("Flamingo outperforms baseline.", file_name="test.search", epoch=1200)

        kb_store.compact_file(
            "test.search", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # BM25 search should find the summary containing "flamingo"
        results = kb_store.search("flamingo", mode="bm25")
        assert len(results) >= 1
        assert any("flamingo" in r.content.lower() for r in results)
        # Original entries should NOT appear (they're in archive)
        source_results = [r for r in results if r.file_name == "test.search"]
        assert len(source_results) == 1  # just the summary

    def test_compact_reindex_consistency(self, kb_store):
        """After compaction + reindex, DB matches file state."""
        self._make_file_with_entries(
            kb_store, "test.reindex",
            [1000, 1100, 1200, 50000, 50100, 50200],
        )
        kb_store.compact_file(
            "test.reindex", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # Reindex from disk
        kb_store.reindex()
        # Source file should have 2 summary entries
        data = kb_store.show_file("test.reindex")
        assert len(data["entries"]) == 2
        # Archive should have 6 originals
        archive_data = kb_store.show_file("test.reindex.archive")
        assert len(archive_data["entries"]) == 6

    def test_compact_double_compaction_idempotent(self, kb_store):
        """Running compact twice — second run is a no-op."""
        self._make_file_with_entries(
            kb_store, "test.double",
            [1000, 1100, 1200],
        )
        result1 = kb_store.compact_file(
            "test.double", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result1["compacted_clusters"] == 1
        # Second run: summary entry is a single entry, not >= min_cluster
        result2 = kb_store.compact_file(
            "test.double", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result2["compacted_clusters"] == 0
        assert result2["entries_remaining"] == 1

    def test_compact_with_existing_archive(self, kb_store):
        """Compaction appends to pre-existing .archive file."""
        kb_store.create_file(name="test.prearchive", description="d", keywords=["test"])
        kb_store.add_entry("Manual archive.", file_name="test.prearchive", epoch=500)
        kb_store.add_entry("Cluster a.", file_name="test.prearchive", epoch=1000)
        kb_store.add_entry("Cluster b.", file_name="test.prearchive", epoch=1100)
        kb_store.add_entry("Cluster c.", file_name="test.prearchive", epoch=1200)
        # Manually archive one entry first
        kb_store.archive_entry("test.prearchive", 500)
        archive_data_before = kb_store.show_file("test.prearchive.archive")
        assert len(archive_data_before["entries"]) == 1

        kb_store.compact_file(
            "test.prearchive", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # Archive should now have 1 + 3 = 4 entries
        archive_data_after = kb_store.show_file("test.prearchive.archive")
        assert len(archive_data_after["entries"]) == 4

    def test_compact_large_cluster(self, kb_store):
        """20-entry cluster compacts correctly."""
        epochs = [1000 + i * 60 for i in range(20)]  # 1-min apart
        self._make_file_with_entries(kb_store, "test.large", epochs)
        result = kb_store.compact_file(
            "test.large", gap_seconds=3600, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result["compacted_clusters"] == 1
        assert result["entries_archived"] == 20
        assert result["summaries_created"] == 1
        assert result["entries_remaining"] == 1
        # Summary contains all 20 entries' content
        data = kb_store.show_file("test.large")
        summary = data["entries"][0]["content"]
        assert "[Compacted 20 entries" in summary
        assert "Entry 0 content." in summary
        assert "Entry 19 content." in summary
        # All 20 in archive
        archive_data = kb_store.show_file("test.large.archive")
        assert len(archive_data["entries"]) == 20

    def test_compact_multiple_eligible_clusters(self, kb_store):
        """Two separate eligible clusters compacted in one call."""
        # Cluster A: [1000, 1100, 1200], gap, Cluster B: [50000, 50100, 50200]
        kb_store.create_file(name="test.multi", description="d", keywords=["test"])
        for ep in [1000, 1100, 1200, 50000, 50100, 50200]:
            kb_store.add_entry(f"Entry at {ep}.", file_name="test.multi", epoch=ep)

        result = kb_store.compact_file(
            "test.multi", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result["compacted_clusters"] == 2
        assert result["entries_archived"] == 6
        assert result["summaries_created"] == 2
        assert result["entries_remaining"] == 2
        # Both summaries in source file
        data = kb_store.show_file("test.multi")
        assert len(data["entries"]) == 2
        assert data["entries"][0]["epoch"] == 1000
        assert data["entries"][1]["epoch"] == 50000

    def test_compact_summary_separators(self, kb_store):
        """Summary entries have --- separators between originals."""
        self._make_file_with_entries(
            kb_store, "test.sep",
            [1000, 1100, 1200],
        )
        kb_store.compact_file(
            "test.sep", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        data = kb_store.show_file("test.sep")
        content = data["entries"][0]["content"]
        # Should have exactly 2 separators (between 3 entries)
        assert content.count("\n---\n") == 2

    def test_compact_file_with_md_suffix(self, kb_store):
        """Passing file_name with .md suffix still works."""
        self._make_file_with_entries(
            kb_store, "test.suffix",
            [1000, 1100, 1200],
        )
        result = kb_store.compact_file(
            "test.suffix.md", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=True,
        )
        assert result["file"] == "test.suffix"
        assert result["total_entries"] == 3

    def test_compact_rejects_archive_file(self, kb_store):
        """compact_file() raises ValueError on archive file names."""
        # Create file so the archive would exist
        self._make_file_with_entries(kb_store, "test.data", [1000, 1100, 1200])
        with pytest.raises(ValueError, match="Cannot compact archive file"):
            kb_store.compact_file("test.data.archive", dry_run=True)
        # Also with .md suffix
        with pytest.raises(ValueError, match="Cannot compact archive file"):
            kb_store.compact_file("test.data.archive.md", dry_run=True)

    def test_compact_double_compaction_with_reindex(self, kb_store):
        """Compact → reindex → compact again must not create duplicate archive epochs."""
        from hyperkb.format import parse_file

        self._make_file_with_entries(
            kb_store, "test.double",
            [1000, 1100, 1200, 50000, 50100, 50200],
        )
        # First compaction
        kb_store.compact_file(
            "test.double", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # Reindex (rebuilds DB from disk)
        kb_store.reindex()
        # Add more entries and compact again
        kb_store.add_entry("New entry A", file_name="test.double", epoch=100000)
        kb_store.add_entry("New entry B", file_name="test.double", epoch=100100)
        kb_store.add_entry("New entry C", file_name="test.double", epoch=100200)
        kb_store.compact_file(
            "test.double", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # Verify archive has no duplicate epochs
        archive_path = kb_store.storage_dir / "test.double.archive.md"
        _, arch_entries = parse_file(archive_path)
        epochs = [e.epoch for e in arch_entries]
        assert len(epochs) == len(set(epochs)), f"Duplicate epochs in archive: {epochs}"
        # Reindex again should succeed without crash
        kb_store.reindex()

    def test_compact_deduplicates_archive_entries(self, kb_store):
        """Pre-populated archive epochs are skipped during compaction."""
        from hyperkb.format import parse_file

        self._make_file_with_entries(
            kb_store, "test.dedup",
            [1000, 1100, 1200],
        )
        # First compaction moves entries to archive
        result1 = kb_store.compact_file(
            "test.dedup", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        assert result1["entries_archived"] == 3
        archive_path = kb_store.storage_dir / "test.dedup.archive.md"
        _, arch1 = parse_file(archive_path)
        count_before = len(arch1)

        # Reindex so the summary entry is in DB
        kb_store.reindex()

        # Now the source file has one summary entry with epoch=1000 (oldest in cluster).
        # Compacting again with min_cluster=1 would try to archive it again.
        result2 = kb_store.compact_file(
            "test.dedup", gap_seconds=5000, min_cluster=1, min_age_seconds=0,
            dry_run=False,
        )
        # The summary entry's epoch=1000 already exists in archive, so 0 new archived
        assert result2["entries_archived"] == 0
        _, arch2 = parse_file(archive_path)
        assert len(arch2) == count_before

    def test_compact_add_compact_archive_integrity(self, kb_store):
        """Full lifecycle: create, add, compact, add more, compact. Archive stays clean."""
        from hyperkb.format import parse_file

        self._make_file_with_entries(
            kb_store, "test.lifecycle",
            [1000, 1100, 1200],
        )
        kb_store.compact_file(
            "test.lifecycle", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # Add more entries
        for i, ep in enumerate([2000, 2100, 2200]):
            kb_store.add_entry(f"Round 2 entry {i}", file_name="test.lifecycle", epoch=ep)
        kb_store.compact_file(
            "test.lifecycle", gap_seconds=5000, min_cluster=3, min_age_seconds=0,
            dry_run=False,
        )
        # Check archive integrity
        archive_path = kb_store.storage_dir / "test.lifecycle.archive.md"
        _, arch_entries = parse_file(archive_path)
        epochs = [e.epoch for e in arch_entries]
        assert len(epochs) == len(set(epochs)), f"Duplicate epochs: {epochs}"
        assert len(arch_entries) == 6  # 3 from first + 3 from second compaction


class TestReindexDuplicateEpochs:
    """Test that reindex() survives duplicate epoch markers on disk."""

    def test_reindex_survives_duplicate_epochs(self, kb_store):
        """Manually-created file with duplicate epochs doesn't crash reindex."""
        from hyperkb.format import FileHeader, create_file_content, Entry

        header = FileHeader(
            name="test.dupes", description="dupe test",
            keywords=[], links=[],
        )
        e1 = Entry(epoch=5000, content="First occurrence")
        e2 = Entry(epoch=5000, content="Duplicate epoch")
        e3 = Entry(epoch=6000, content="Normal entry")

        # Write file with duplicate epoch manually
        path = kb_store.storage_dir / "test.dupes.md"
        text = create_file_content(header, [e1])
        # Append duplicate epoch entry manually
        text += f"\n>>> 5000\nDuplicate epoch\n"
        text += f"\n>>> 6000\nNormal entry\n"
        path.write_text(text, encoding="utf-8")

        # Reindex should not crash
        result = kb_store.reindex()
        assert "test.dupes" in result or "1 files" in result or "files" in result


class TestWriteTimeEntryLinks:
    """Tests for entry_links populated at write time (_append_entry, update_entry)."""

    def test_append_entry_populates_entry_links(self, kb_store):
        """Adding an entry with [[wiki-link]] populates entry_links immediately."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]] for details", file_name="a.b", epoch=100)
        refs = kb_store.db.get_entry_references("a.b", 100)
        assert len(refs) == 1
        assert refs[0]["target_file"] == "c.d"
        assert refs[0]["link_type"] == "file"

    def test_append_entry_no_links(self, kb_store):
        """Entry without wiki-links creates no entry_links rows."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("Plain content", file_name="a.b", epoch=100)
        refs = kb_store.db.get_entry_references("a.b", 100)
        assert len(refs) == 0

    def test_append_entry_epoch_link(self, kb_store):
        """[[file#epoch]] creates entry-level link at write time."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d#500]]", file_name="a.b", epoch=100)
        refs = kb_store.db.get_entry_references("a.b", 100)
        entry_refs = [r for r in refs if r["link_type"] == "entry"]
        assert len(entry_refs) == 1
        assert entry_refs[0]["target_file"] == "c.d"
        assert entry_refs[0]["target_epoch"] == 500

    def test_update_entry_refreshes_entry_links(self, kb_store):
        """Updating entry content re-extracts wiki-links."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.create_file(name="e.f", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        # Initially links to c.d
        refs = kb_store.db.get_entry_references("a.b", 100)
        assert any(r["target_file"] == "c.d" for r in refs)
        # Update to link to e.f instead
        kb_store.update_entry("a.b", 100, new_content="See [[e.f]]")
        refs = kb_store.db.get_entry_references("a.b", 100)
        target_files = [r["target_file"] for r in refs]
        assert "e.f" in target_files
        assert "c.d" not in target_files

    def test_update_entry_status_only_preserves_links(self, kb_store):
        """Updating only status (not content) does NOT touch entry_links."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        refs_before = kb_store.db.get_entry_references("a.b", 100)
        kb_store.update_entry("a.b", 100, set_status="superseded")
        refs_after = kb_store.db.get_entry_references("a.b", 100)
        assert len(refs_before) == len(refs_after)


class TestSyncEntryLinks:
    """Tests for sync_entry_links() startup sync."""

    def test_sync_populates_empty_table(self, kb_store):
        """sync_entry_links populates links when table is empty but entries exist."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        # Clear the links that _append_entry just created
        kb_store.db.clear_entry_links()
        assert kb_store.db.count_entry_links() == 0
        # Sync should repopulate
        kb_store.sync_entry_links()
        assert kb_store.db.count_entry_links() > 0
        refs = kb_store.db.get_entry_references("a.b", 100)
        assert any(r["target_file"] == "c.d" for r in refs)

    def test_sync_skips_when_links_exist(self, kb_store):
        """sync_entry_links is a no-op when entry_links already populated."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        count_before = kb_store.db.count_entry_links()
        assert count_before > 0
        # Sync again — should not double the count
        kb_store.sync_entry_links()
        assert kb_store.db.count_entry_links() == count_before

    def test_sync_skips_when_no_entries(self, kb_store):
        """sync_entry_links is a no-op when there are no entries."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.sync_entry_links()
        assert kb_store.db.count_entry_links() == 0

    def test_sync_skips_archive_files(self, kb_store):
        """sync_entry_links only reads non-archive files."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        kb_store.archive_entry("a.b", 100)
        # Clear links and re-sync
        kb_store.db.clear_entry_links()
        kb_store.sync_entry_links()
        # The archived entry shouldn't generate links since sync skips .archive files
        # But the source file now has 0 entries, so no links from it either
        assert kb_store.db.count_entry_links() == 0


class TestHealthCheck:
    """Tests for health_snapshot() and health_check() methods."""

    @pytest.fixture
    def unhealthy_kb(self, kb_store):
        """A KB with intentional issues for health checks to detect."""
        import time as _time

        # Create files
        kb_store.create_file(name="a.b", description="File A", keywords=["alpha"])
        kb_store.create_file(name="c.d", description="File C", keywords=["gamma"])

        # Add entries with various issues:
        # 1. A broken outbound link (to nonexistent.file)
        kb_store.add_entry("See [[nonexistent.file]] for more", file_name="a.b", epoch=100)
        # 2. A self-link
        kb_store.add_entry("See [[a.b]] for self", file_name="a.b", epoch=200)
        # 3. A normal entry (no issues except no tags)
        kb_store.add_entry("Normal content here", file_name="c.d", epoch=300)
        # 4. Old entry for stale_active check (epoch from 2020)
        kb_store.add_entry("Very old entry", file_name="c.d", epoch=1577836800)
        # 5. Add a misplaced archived entry directly in DB
        kb_store.db.insert_entry("a.b", 400, "misplaced archived", status="archived")

        # 6. Add orphan entry_links (source entry doesn't exist)
        kb_store.db.insert_entry_links([("a.b", 99999, "c.d", 0, "file")])

        return kb_store

    # --- health_snapshot (Tier 1) ---

    def test_snapshot_healthy_kb(self, kb_store):
        """Clean KB gets all-ok snapshot."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("content", file_name="a.b", epoch=100)
        result = kb_store.health_snapshot()
        assert result["summary"]["checks_run"] == 4
        assert result["summary"]["error"] == 0

    def test_snapshot_returns_checks(self, kb_store):
        """Snapshot returns expected check names."""
        result = kb_store.health_snapshot()
        names = {c["name"] for c in result["checks"]}
        assert "entry_links_coverage" in names
        assert "orphan_entry_links" in names
        assert "db_vs_disk_file_count" in names
        assert "empty_files" in names

    def test_snapshot_detects_orphan_links(self, unhealthy_kb):
        result = unhealthy_kb.health_snapshot()
        orphan_check = next(c for c in result["checks"] if c["name"] == "orphan_entry_links")
        assert orphan_check["status"] == "warning"
        assert orphan_check["count"] >= 1

    # --- Individual Tier 1 checks ---

    def test_check_entry_links_coverage_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("See [[a.b]]", file_name="a.b", epoch=100)
        result = kb_store._check_entry_links_coverage()
        # There's at least one entry and at least one link
        assert result["status"] == "ok"

    def test_check_entry_links_coverage_empty(self, kb_store):
        """Entries exist but no links → warning."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("plain text", file_name="a.b", epoch=100)
        # Clear entry_links to simulate empty table
        kb_store.db.clear_entry_links()
        result = kb_store._check_entry_links_coverage()
        assert result["status"] == "warning"

    def test_check_orphan_entry_links_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("See [[a.b]]", file_name="a.b", epoch=100)
        result = kb_store._check_orphan_entry_links()
        assert result["status"] == "ok"

    def test_check_orphan_entry_links_detected(self, unhealthy_kb):
        result = unhealthy_kb._check_orphan_entry_links()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_db_vs_disk_file_count_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        result = kb_store._check_db_vs_disk_file_count()
        assert result["status"] == "ok"

    def test_check_db_vs_disk_file_count_mismatch(self, kb_store):
        """File on disk but not in DB → warning."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        # Write a file to disk without indexing it
        rogue = kb_store.storage_dir / "rogue.file.md"
        rogue.write_text("---\nname: rogue.file\n---\n", encoding="utf-8")
        result = kb_store._check_db_vs_disk_file_count()
        assert result["status"] != "ok"
        assert result["count"] >= 1

    def test_check_empty_files_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("content", file_name="a.b", epoch=100)
        result = kb_store._check_empty_files()
        assert result["status"] == "ok"

    def test_check_empty_files_detected(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        result = kb_store._check_empty_files()
        assert result["status"] == "warning"
        assert result["count"] == 1

    # --- Individual Tier 2 checks ---

    def test_check_broken_outbound_links(self, unhealthy_kb):
        result = unhealthy_kb._check_broken_outbound_links()
        assert result["status"] == "warning"
        targets = [d["target"] for d in result["details"]]
        assert "nonexistent.file" in targets

    def test_check_broken_outbound_links_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        result = kb_store._check_broken_outbound_links()
        assert result["status"] == "ok"

    def test_check_self_links(self, unhealthy_kb):
        result = unhealthy_kb._check_self_links()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_self_links_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        result = kb_store._check_self_links()
        assert result["status"] == "ok"

    def test_check_header_link_integrity(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[], links=["nonexistent.file"])
        result = kb_store._check_header_link_integrity()
        assert result["status"] == "warning"
        assert result["count"] == 1

    def test_check_header_link_integrity_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[], links=["a.b"])
        result = kb_store._check_header_link_integrity()
        assert result["status"] == "ok"

    def test_check_misplaced_archived(self, unhealthy_kb):
        result = unhealthy_kb._check_misplaced_archived()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_misplaced_archived_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("content", file_name="a.b", epoch=100)
        result = kb_store._check_misplaced_archived()
        assert result["status"] == "ok"

    def test_check_compaction_readiness_ok(self, kb_store):
        """No files ready → ok."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("content", file_name="a.b", epoch=100)
        result = kb_store._check_compaction_readiness()
        assert result["status"] == "ok"

    def test_check_disk_db_entry_drift(self, kb_store):
        """Drift when DB has extra entry not on disk."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("content", file_name="a.b", epoch=100)
        # Add entry to DB only (not on disk)
        kb_store.db.insert_entry("a.b", 999, "ghost entry")
        result = kb_store._check_disk_db_entry_drift()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_disk_db_entry_drift_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("content", file_name="a.b", epoch=100)
        result = kb_store._check_disk_db_entry_drift()
        assert result["status"] == "ok"

    # --- Individual Tier 3 checks ---

    def test_check_stale_active(self, unhealthy_kb):
        result = unhealthy_kb._check_stale_active()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_stale_active_ok(self, kb_store):
        import time as _time
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("recent", file_name="a.b", epoch=int(_time.time()))
        result = kb_store._check_stale_active()
        assert result["status"] == "ok"

    def test_check_untagged_entries(self, unhealthy_kb):
        result = unhealthy_kb._check_untagged_entries()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_untagged_entries_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("@tags: important\ncontent", file_name="a.b", epoch=100)
        result = kb_store._check_untagged_entries()
        assert result["status"] == "ok"

    def test_check_potential_duplicates(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("Exact same content here", file_name="a.b", epoch=100)
        kb_store.add_entry("Exact same content here", file_name="a.b", epoch=200)
        result = kb_store._check_potential_duplicates()
        assert result["status"] == "warning"
        assert result["count"] >= 1

    def test_check_potential_duplicates_ok(self, kb_store):
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.add_entry("First unique entry", file_name="a.b", epoch=100)
        kb_store.add_entry("Second unique entry", file_name="a.b", epoch=200)
        result = kb_store._check_potential_duplicates()
        assert result["status"] == "ok"

    # --- Orchestrators ---

    def test_health_check_all_tiers(self, unhealthy_kb):
        """Full health check returns all 13 checks."""
        result = unhealthy_kb.health_check(include_tier3=True)
        assert result["summary"]["checks_run"] == 13
        assert result["summary"]["total_issues"] > 0

    def test_health_check_no_tier3(self, unhealthy_kb):
        """Excluding tier 3 returns 10 checks."""
        result = unhealthy_kb.health_check(include_tier3=False)
        assert result["summary"]["checks_run"] == 10

    def test_health_check_fix_orphan_links(self, unhealthy_kb):
        """fix=True should delete orphan entry_links."""
        result = unhealthy_kb.health_check(fix=True)
        assert "fixes_applied" in result
        fix_names = [f["name"] for f in result["fixes_applied"]]
        assert "orphan_entry_links" in fix_names
        # Verify orphans are actually gone
        orphans = unhealthy_kb.db.get_orphan_entry_links()
        assert len(orphans) == 0

    def test_health_check_fix_entry_links_coverage(self, kb_store):
        """fix=True syncs entry_links when table is empty."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        # Clear entry_links to trigger the fix
        kb_store.db.clear_entry_links()
        assert kb_store.db.count_entry_links() == 0
        result = kb_store.health_check(fix=True)
        fix_names = [f["name"] for f in result.get("fixes_applied", [])]
        assert "entry_links_coverage" in fix_names

    def test_health_check_no_fix_when_ok(self, kb_store):
        """fix=True on clean KB doesn't produce any fixes."""
        kb_store.create_file(name="a.b", description="d", keywords=[])
        kb_store.create_file(name="c.d", description="d", keywords=[])
        # Entry with a wiki-link so entry_links table is populated
        kb_store.add_entry("See [[c.d]]", file_name="a.b", epoch=100)
        result = kb_store.health_check(fix=True)
        fixes = result.get("fixes_applied", [])
        assert len(fixes) == 0

    def test_compile_health_result(self):
        """Static method compiles summary from check list."""
        checks = [
            {"name": "a", "status": "ok", "count": 0, "details": [], "message": "", "fix_hint": ""},
            {"name": "b", "status": "warning", "count": 3, "details": [], "message": "", "fix_hint": ""},
            {"name": "c", "status": "error", "count": 1, "details": [], "message": "", "fix_hint": ""},
        ]
        result = KnowledgeStore._compile_health_result(checks)
        assert result["summary"]["checks_run"] == 3
        assert result["summary"]["ok"] == 1
        assert result["summary"]["warning"] == 1
        assert result["summary"]["error"] == 1
        assert result["summary"]["total_issues"] == 4


class TestAtomicReindex:
    def test_reindex_preserves_data(self, sample_kb):
        """Reindex returns correct counts and all data is present afterward."""
        result = sample_kb.reindex()
        assert "2 files" in result
        assert "3 entries" in result
        # Verify data is actually there
        files = sample_kb.list_files()
        assert len(files) == 2
        conn = sample_kb.db.connect()
        entry_count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        assert entry_count == 3

    def test_reindex_rollback_on_failure(self, kb_store, monkeypatch):
        """If an error occurs mid-reindex, old data is preserved via rollback."""
        import hyperkb.store as store_mod

        kb_store.create_file(name="test.file", description="Survives", keywords=["k"])
        kb_store.add_entry("important data", file_name="test.file", epoch=100)

        # Verify baseline
        conn = kb_store.db.connect()
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1

        # Inject failure: make _build_entry_links raise during reindex
        def exploding_build(*args, **kwargs):
            raise RuntimeError("injected failure in entry links")

        monkeypatch.setattr(store_mod, "_build_entry_links", exploding_build)

        with pytest.raises(RuntimeError, match="injected failure"):
            kb_store.reindex()

        # Old data should be preserved because the transaction rolled back
        assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0] == 1

    def test_reindex_no_executescript(self):
        """Reindex source code does not use executescript (which auto-commits per statement)."""
        import inspect
        source = inspect.getsource(KnowledgeStore.reindex)
        assert "executescript" not in source
