"""Tests for hyperkb.db — schema, CRUD, FTS5, epoch collision, time filtering."""

import json
import pytest

from hyperkb.db import KBDatabase, MAX_EPOCH_RETRIES, TimeoutLock
from hyperkb.models import FileHeader


class TestSchema:
    def test_init_schema_creates_tables(self, kb_db):
        conn = kb_db.conn
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "files" in tables
        assert "entries" in tables

    def test_init_schema_idempotent(self, kb_db):
        # Calling init_schema again should not error
        kb_db.init_schema()

    def test_busy_timeout_is_set(self, kb_db):
        """Connection should have busy_timeout to handle concurrent access."""
        row = kb_db.conn.execute("PRAGMA busy_timeout").fetchone()
        assert row[0] >= 5000

    def test_db_lock_is_timeout_lock(self, kb_db):
        """DB lock must be a TimeoutLock, not a bare threading.Lock."""
        assert isinstance(kb_db._lock, TimeoutLock)
        assert kb_db._lock._timeout == 60


class TestTimeoutLock:
    def test_acquire_and_release(self):
        lock = TimeoutLock(timeout=1, name="test")
        with lock:
            pass  # should not raise

    def test_timeout_raises(self):
        lock = TimeoutLock(timeout=0.1, name="test")
        lock.__enter__()  # hold the lock
        try:
            with pytest.raises(TimeoutError, match="test lock"):
                lock.__enter__()  # second acquire should timeout
        finally:
            lock.__exit__(None, None, None)

    def test_release_allows_reacquire(self):
        lock = TimeoutLock(timeout=0.1, name="test")
        with lock:
            pass
        # Should be reacquirable after release
        with lock:
            pass


class TestFileOperations:
    def test_insert_and_get_file(self, kb_db):
        header = FileHeader(
            name="test.file",
            description="A test file",
            keywords=["test", "file"],
            links=["other.file"],
            created="2026-01-01T00:00:00Z",
        )
        kb_db.insert_file(header, "test.file.md")
        result = kb_db.get_file("test.file")
        assert result is not None
        assert result["name"] == "test.file"
        assert result["description"] == "A test file"
        assert json.loads(result["keywords"]) == ["test", "file"]

    def test_file_exists(self, kb_db):
        header = FileHeader(name="exists.file", description="d")
        kb_db.insert_file(header, "exists.file.md")
        assert kb_db.file_exists("exists.file") is True
        assert kb_db.file_exists("nonexistent.file") is False

    def test_list_files(self, kb_db):
        for name in ["a.b", "a.c", "b.d"]:
            header = FileHeader(name=name, description=f"desc of {name}")
            kb_db.insert_file(header, f"{name}.md")
        files = kb_db.list_files()
        assert len(files) == 3

    def test_list_files_with_domain(self, kb_db):
        for name in ["sec.a", "sec.b", "fit.c"]:
            header = FileHeader(name=name, description=f"desc of {name}")
            kb_db.insert_file(header, f"{name}.md")
        files = kb_db.list_files(domain_prefix="sec")
        assert len(files) == 2
        assert all(f["name"].startswith("sec") for f in files)

    def test_list_files_includes_entry_count(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 1000, "entry one")
        kb_db.insert_entry("test.file", 2000, "entry two")
        files = kb_db.list_files()
        assert files[0]["entry_count"] == 2
        assert files[0]["latest_epoch"] == 2000

    def test_list_files_empty_has_zero_count(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        files = kb_db.list_files()
        assert files[0]["entry_count"] == 0
        assert files[0]["latest_epoch"] is None

    def test_get_all_file_summaries(self, kb_db):
        header = FileHeader(name="test.file", description="d", keywords=["k"])
        kb_db.insert_file(header, "test.file.md")
        summaries = kb_db.get_all_file_summaries()
        assert len(summaries) == 1
        assert summaries[0]["name"] == "test.file"


class TestEntryOperations:
    def test_insert_and_get_entries(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 1000, "entry content")
        entries = kb_db.get_entries("test.file")
        assert len(entries) == 1
        assert entries[0]["epoch"] == 1000
        assert entries[0]["content"] == "entry content"

    def test_epoch_collision_increments(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        _, epoch1 = kb_db.insert_entry("test.file", 1000, "first")
        _, epoch2 = kb_db.insert_entry("test.file", 1000, "second")
        assert epoch1 == 1000
        assert epoch2 == 1001  # auto-incremented

    def test_epoch_collision_retry_bound(self, kb_db):
        """Inserting beyond MAX_EPOCH_RETRIES collisions raises RuntimeError."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        base_epoch = 5000
        # Fill up MAX_EPOCH_RETRIES + 1 consecutive epochs
        for i in range(MAX_EPOCH_RETRIES + 1):
            kb_db.insert_entry("test.file", base_epoch + i, f"entry {i}")
        with pytest.raises(RuntimeError, match="Epoch collision"):
            kb_db.insert_entry("test.file", base_epoch, "should fail")

    def test_delete_entry_by_epoch(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 1000, "to be deleted")
        kb_db.insert_entry("test.file", 2000, "to be kept")

        assert kb_db.delete_entry_by_epoch("test.file", 1000) is True
        entries = kb_db.get_entries("test.file")
        assert len(entries) == 1
        assert entries[0]["epoch"] == 2000

    def test_delete_entry_by_epoch_not_found(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        assert kb_db.delete_entry_by_epoch("test.file", 9999) is False

    def test_delete_entry_by_epoch_removes_fts(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 1000, "unique searchable canary")

        # Verify it's searchable
        results = kb_db.bm25_search_entries("canary")
        assert len(results) >= 1

        # Delete and verify FTS is cleaned up
        kb_db.delete_entry_by_epoch("test.file", 1000)
        results = kb_db.bm25_search_entries("canary")
        assert len(results) == 0

    def test_get_entries_after_epoch(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "old")
        kb_db.insert_entry("test.file", 200, "new")
        entries = kb_db.get_entries("test.file", after_epoch=150)
        assert len(entries) == 1
        assert entries[0]["epoch"] == 200

    def test_get_entries_after_epoch_zero(self, kb_db):
        """Epoch 0 edge case: after_epoch=0 should filter properly."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 0, "epoch-zero")
        kb_db.insert_entry("test.file", 100, "later")
        entries = kb_db.get_entries("test.file", after_epoch=0)
        assert len(entries) == 1
        assert entries[0]["epoch"] == 100

    def test_get_entries_last_n(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        for i in range(5):
            kb_db.insert_entry("test.file", i * 100, f"entry {i}")
        entries = kb_db.get_entries("test.file", last_n=2)
        assert len(entries) == 2
        assert entries[0]["epoch"] == 300
        assert entries[1]["epoch"] == 400

    def test_get_entries_last_n_zero(self, kb_db):
        """last_n=0 should return 0 entries."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "e")
        entries = kb_db.get_entries("test.file", last_n=0)
        assert len(entries) == 0

    def test_get_entries_combined_filters(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        for i in range(10):
            kb_db.insert_entry("test.file", i * 100, f"entry {i}")
        # after 400, last 2 → entries at 800 and 900
        entries = kb_db.get_entries("test.file", after_epoch=400, last_n=2)
        assert len(entries) == 2
        assert entries[0]["epoch"] == 800
        assert entries[1]["epoch"] == 900


class TestUpdateFileMetadata:
    def test_update_description(self, kb_db):
        header = FileHeader(name="test.file", description="old desc", keywords=["k"])
        kb_db.insert_file(header, "test.file.md")
        assert kb_db.update_file_metadata("test.file", description="new desc") is True
        f = kb_db.get_file("test.file")
        assert f["description"] == "new desc"

    def test_update_keywords(self, kb_db):
        header = FileHeader(name="test.file", description="d", keywords=["old"])
        kb_db.insert_file(header, "test.file.md")
        kb_db.update_file_metadata("test.file", keywords=["new", "updated"])
        f = kb_db.get_file("test.file")
        import json
        assert json.loads(f["keywords"]) == ["new", "updated"]

    def test_update_not_found(self, kb_db):
        assert kb_db.update_file_metadata("nonexistent.file", description="d") is False

    def test_update_reflects_in_fts(self, kb_db):
        header = FileHeader(name="test.file", description="original boring", keywords=["old"])
        kb_db.insert_file(header, "test.file.md")
        kb_db.update_file_metadata("test.file", description="unique unicorn rainbow")
        results = kb_db.bm25_search_files("unicorn")
        assert len(results) >= 1
        assert results[0]["name"] == "test.file"

    def test_update_old_terms_removed_from_fts(self, kb_db):
        header = FileHeader(name="test.file", description="flamingo", keywords=[])
        kb_db.insert_file(header, "test.file.md")
        kb_db.update_file_metadata("test.file", description="penguin")
        # Old term should no longer match
        results = kb_db.bm25_search_files("flamingo")
        assert len(results) == 0

    def test_update_links(self, kb_db):
        header = FileHeader(name="test.file", description="d", links=["old.link"])
        kb_db.insert_file(header, "test.file.md")
        kb_db.update_file_metadata("test.file", links=["new.link", "another.link"])
        f = kb_db.get_file("test.file")
        import json
        assert json.loads(f["links"]) == ["new.link", "another.link"]


class TestBM25Search:
    def test_basic_search(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "AlienVault OTX has 6hr delay")
        results = kb_db.bm25_search_entries("AlienVault")
        assert len(results) >= 1
        assert results[0].file_name == "test.file"
        assert results[0].source == "bm25"

    def test_no_results(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "some content")
        results = kb_db.bm25_search_entries("xyznonexistent")
        assert len(results) == 0

    def test_fts5_sanitization(self, kb_db):
        """Special characters should not crash FTS5."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "test content")
        # These should not raise
        kb_db.bm25_search_entries("test*")
        kb_db.bm25_search_entries("test OR")
        kb_db.bm25_search_entries("")
        kb_db.bm25_search_entries("***")

    def test_search_with_after_epoch(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "old content")
        kb_db.insert_entry("test.file", 500, "new content")
        results = kb_db.bm25_search_entries("content", after_epoch=200)
        assert len(results) == 1
        assert results[0].epoch == 500

    def test_search_with_before_epoch(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "old content")
        kb_db.insert_entry("test.file", 500, "new content")
        results = kb_db.bm25_search_entries("content", before_epoch=300)
        assert len(results) == 1
        assert results[0].epoch == 100

    def test_search_with_both_epoch_filters(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "content A")
        kb_db.insert_entry("test.file", 300, "content B")
        kb_db.insert_entry("test.file", 500, "content C")
        results = kb_db.bm25_search_entries("content", after_epoch=150, before_epoch=450)
        assert len(results) == 1
        assert results[0].epoch == 300


    def test_bm25_search_with_offset(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        for i in range(5):
            kb_db.insert_entry("test.file", 100 + i, f"searchterm entry {i}")
        # Without offset: should return all 5
        all_results = kb_db.bm25_search_entries("searchterm", limit=10)
        assert len(all_results) == 5
        # With offset=2, limit=2: should return 2 results, skipping first 2
        paged = kb_db.bm25_search_entries("searchterm", limit=2, offset=2)
        assert len(paged) == 2
        # The paged results should be different from the first 2
        first_page = kb_db.bm25_search_entries("searchterm", limit=2, offset=0)
        assert paged[0].epoch != first_page[0].epoch

    def test_bm25_results_include_metadata(self, kb_db):
        """BM25 SearchResults should have status, entry_type, tags populated."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry(
            "test.file", 100, "important decision content",
            status="active", entry_type="decision", tags="arch,design",
        )
        results = kb_db.bm25_search_entries("decision")
        assert len(results) >= 1
        r = results[0]
        assert r.status == "active"
        assert r.entry_type == "decision"
        assert r.tags == "arch,design"


class TestBM25MultiTermOR:
    """Multi-term BM25 queries should use OR semantics, matching ANY term."""

    def test_multi_term_matches_any_term(self, kb_db):
        """A 3+ term query should find entries matching any single term."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "aibox GPU server setup")
        kb_db.insert_entry("test.file", 200, "ventibean coffee roaster notes")
        kb_db.insert_entry("test.file", 300, "completely unrelated topic")
        results = kb_db.bm25_search_entries("aibox ventibean GPU remote ssh")
        # Should find at least the 2 entries that match individual terms
        assert len(results) >= 2
        epochs = {r.epoch for r in results}
        assert 100 in epochs  # matches aibox, GPU
        assert 200 in epochs  # matches ventibean

    def test_multi_term_does_not_require_all_terms(self, kb_db):
        """An entry matching only 1 of 5 terms should still be returned."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "aibox is a GPU server")
        results = kb_db.bm25_search_entries("aibox ventibean remote ssh docker")
        assert len(results) >= 1
        assert results[0].epoch == 100

    def test_more_matching_terms_rank_higher(self, kb_db):
        """Entry matching 3 terms should rank above entry matching 1 term."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "aibox is a server")
        kb_db.insert_entry("test.file", 200, "aibox GPU remote access setup")
        results = kb_db.bm25_search_entries("aibox GPU remote")
        assert len(results) >= 2
        # The entry matching all 3 terms should rank first
        assert results[0].epoch == 200


class TestFTS5KeywordQuoting:
    def test_search_literal_NOT(self, kb_db):
        """Searching for the literal word 'NOT' should return results, not interpret as FTS5 operator."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "This is NOT a drill")
        results = kb_db.bm25_search_entries("NOT")
        assert len(results) >= 1
        assert "NOT" in results[0].content

    def test_search_literal_OR(self, kb_db):
        """Searching for 'OR' should return results containing the word."""
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "Use OR logic in the query")
        results = kb_db.bm25_search_entries("OR")
        assert len(results) >= 1


class TestBM25FileSearch:
    def test_search_files(self, kb_db):
        header = FileHeader(
            name="security.threats",
            description="Threat intelligence findings",
            keywords=["threat", "intel"],
        )
        kb_db.insert_file(header, "security.threats.md")
        results = kb_db.bm25_search_files("threat intelligence")
        assert len(results) >= 1
        assert results[0]["name"] == "security.threats"


class TestLinks:
    def test_get_links_for_file(self, kb_db):
        h1 = FileHeader(name="a.b", description="d", links=["c.d"])
        h2 = FileHeader(name="c.d", description="d", links=["a.b"])
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "c.d.md")
        kb_db.insert_entry("a.b", 100, "See [[c.d]] for more")

        links = kb_db.get_links_for_file("a.b")
        assert "c.d" in links["outbound"]
        assert "c.d" in links["inbound_header"]  # c.d links back

        links_cd = kb_db.get_links_for_file("c.d")
        assert "a.b" in links_cd["inbound_entries"]  # a.b has wikilink to c.d


class TestEntryMetadata:
    def test_insert_entry_with_metadata(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "content", status="superseded", entry_type="decision", tags="arch, db")
        entries = kb_db.get_entries("test.file")
        assert entries[0]["status"] == "superseded"
        assert entries[0]["entry_type"] == "decision"
        assert entries[0]["tags"] == "arch, db"

    def test_insert_entry_defaults(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "content")
        entries = kb_db.get_entries("test.file")
        assert entries[0]["status"] == "active"
        assert entries[0]["entry_type"] == "note"
        assert entries[0]["tags"] == ""

    def test_search_filter_by_status(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "active findme", status="active")
        kb_db.insert_entry("test.file", 200, "superseded findme", status="superseded")
        results = kb_db.bm25_search_entries("findme", status="active")
        assert len(results) == 1
        assert results[0].epoch == 100

    def test_search_filter_by_type(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "note findme", entry_type="note")
        kb_db.insert_entry("test.file", 200, "task findme", entry_type="task")
        results = kb_db.bm25_search_entries("findme", entry_type="task")
        assert len(results) == 1
        assert results[0].epoch == 200

    def test_search_exclude_archives_default(self, kb_db):
        h1 = FileHeader(name="test.file", description="d")
        h2 = FileHeader(name="test.file.archive", description="archived")
        kb_db.insert_file(h1, "test.file.md")
        kb_db.insert_file(h2, "test.file.archive.md")
        kb_db.insert_entry("test.file", 100, "canary active")
        kb_db.insert_entry("test.file.archive", 200, "canary archived")
        results = kb_db.bm25_search_entries("canary", exclude_archives=True)
        assert all(r.file_name != "test.file.archive" for r in results)

    def test_search_include_archives(self, kb_db):
        h1 = FileHeader(name="test.file", description="d")
        h2 = FileHeader(name="test.file.archive", description="archived")
        kb_db.insert_file(h1, "test.file.md")
        kb_db.insert_file(h2, "test.file.archive.md")
        kb_db.insert_entry("test.file", 100, "canary active")
        kb_db.insert_entry("test.file.archive", 200, "canary archived")
        results = kb_db.bm25_search_entries("canary", exclude_archives=False)
        file_names = [r.file_name for r in results]
        assert "test.file.archive" in file_names

    def test_get_entry(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "entry content")
        entry = kb_db.get_entry("test.file", 100)
        assert entry is not None
        assert entry["content"] == "entry content"

    def test_get_entry_not_found(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        assert kb_db.get_entry("test.file", 999) is None

    def test_update_entry_content(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "original")
        assert kb_db.update_entry("test.file", 100, content="updated") is True
        entry = kb_db.get_entry("test.file", 100)
        assert entry["content"] == "updated"

    def test_update_entry_status(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "content", status="active")
        kb_db.update_entry("test.file", 100, status="superseded")
        entry = kb_db.get_entry("test.file", 100)
        assert entry["status"] == "superseded"

    def test_update_entry_not_found(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        assert kb_db.update_entry("test.file", 999, content="x") is False

    def test_update_entry_reflects_in_fts(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "flamingo original")
        kb_db.update_entry("test.file", 100, content="penguin updated")
        # New term should be searchable
        results = kb_db.bm25_search_entries("penguin")
        assert len(results) >= 1
        # Old term should be gone
        results = kb_db.bm25_search_entries("flamingo")
        assert len(results) == 0

    def test_get_recent_entries(self, kb_db):
        header = FileHeader(name="test.file", description="d")
        kb_db.insert_file(header, "test.file.md")
        kb_db.insert_entry("test.file", 100, "old", status="active")
        kb_db.insert_entry("test.file", 200, "new", status="active")
        recent = kb_db.get_recent_entries(limit=10)
        assert len(recent) == 2
        assert recent[0]["epoch"] == 200  # newest first

    def test_get_recent_entries_excludes_archives(self, kb_db):
        h1 = FileHeader(name="test.file", description="d")
        h2 = FileHeader(name="test.file.archive", description="archived")
        kb_db.insert_file(h1, "test.file.md")
        kb_db.insert_file(h2, "test.file.archive.md")
        kb_db.insert_entry("test.file", 100, "active entry", status="active")
        kb_db.insert_entry("test.file.archive", 200, "archived entry", status="active")
        recent = kb_db.get_recent_entries(limit=10)
        assert all(r["file_name"] != "test.file.archive" for r in recent)

    def test_get_tasks(self, kb_db):
        header = FileHeader(name="tasks.test", description="d")
        kb_db.insert_file(header, "tasks.test.md")
        kb_db.insert_entry("tasks.test", 100, "task one", entry_type="task", status="pending")
        kb_db.insert_entry("tasks.test", 200, "task two", entry_type="task", status="completed")
        kb_db.insert_entry("tasks.test", 300, "note one", entry_type="note", status="active")
        # Default: pending tasks only
        tasks = kb_db.get_tasks(status_filter="pending")
        assert len(tasks) == 1
        assert tasks[0]["content"] == "task one"
        # All tasks
        tasks = kb_db.get_tasks(status_filter="all")
        assert len(tasks) == 2  # only task entries, not notes


class TestEntryLinks:
    def test_entry_links_table_exists(self, kb_db):
        conn = kb_db.conn
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert "entry_links" in tables

    def test_insert_and_query_references(self, kb_db):
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry("a.b", 100, "content")
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),
            ("a.b", 100, "e.f", 200, "entry"),
        ])
        refs = kb_db.get_entry_references("a.b", 100)
        assert len(refs) == 2
        targets = [(r["target_file"], r["target_epoch"]) for r in refs]
        assert ("c.d", 0) in targets
        assert ("e.f", 200) in targets

    def test_get_entry_backlinks(self, kb_db):
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry_links([
            ("x.y", 500, "a.b", 100, "entry"),
        ])
        backlinks = kb_db.get_entry_backlinks("a.b", 100)
        assert len(backlinks) == 1
        assert backlinks[0]["source_file"] == "x.y"
        assert backlinks[0]["source_epoch"] == 500

    def test_get_entry_backlinks_includes_file_level(self, kb_db):
        """File-level links (target_epoch=0) appear in epoch-specific backlink queries."""
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry_links([
            ("x.y", 500, "a.b", 0, "file"),       # file-level
            ("x.y", 600, "a.b", 100, "entry"),     # entry-level
        ])
        backlinks = kb_db.get_entry_backlinks("a.b", 100)
        sources = [(b["source_file"], b["source_epoch"]) for b in backlinks]
        assert ("x.y", 500) in sources   # file-level included
        assert ("x.y", 600) in sources   # entry-level included

    def test_get_entry_backlinks_all(self, kb_db):
        """Without epoch, returns all inbound links to the file."""
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry_links([
            ("x.y", 500, "a.b", 0, "file"),
            ("x.y", 600, "a.b", 100, "entry"),
            ("z.w", 700, "a.b", 200, "entry"),
        ])
        backlinks = kb_db.get_entry_backlinks("a.b")
        assert len(backlinks) == 3

    def test_clear_entry_links(self, kb_db):
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),
        ])
        kb_db.clear_entry_links()
        refs = kb_db.get_entry_references("a.b", 100)
        assert len(refs) == 0

    def test_insert_duplicate_links_ignored(self, kb_db):
        link = ("a.b", 100, "c.d", 0, "file")
        kb_db.insert_entry_links([link, link])
        refs = kb_db.get_entry_references("a.b", 100)
        assert len(refs) == 1

    def test_delete_entry_links_for_source(self, kb_db):
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),
            ("a.b", 100, "e.f", 200, "entry"),
            ("x.y", 300, "c.d", 0, "file"),  # different source
        ])
        kb_db.delete_entry_links_for_source("a.b", 100)
        # a.b/100 links should be gone
        refs = kb_db.get_entry_references("a.b", 100)
        assert len(refs) == 0
        # x.y/300 links should remain
        refs = kb_db.get_entry_references("x.y", 300)
        assert len(refs) == 1

    def test_count_entries(self, kb_db):
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        assert kb_db.count_entries() == 0
        kb_db.insert_entry("a.b", 100, "content")
        assert kb_db.count_entries() == 1
        kb_db.insert_entry("a.b", 200, "more content")
        assert kb_db.count_entries() == 2

    def test_count_entry_links(self, kb_db):
        assert kb_db.count_entry_links() == 0
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),
        ])
        assert kb_db.count_entry_links() == 1


class TestLinksLikePrefixBug:
    """Regression: LIKE pattern must not match prefix-similar file names."""

    def test_inbound_entries_no_prefix_false_positive(self, kb_db):
        """Searching for [[a.b]] must not match entries containing [[a.b.c.d]]."""
        h1 = FileHeader(name="a.b", description="d")
        h2 = FileHeader(name="a.b.c.d", description="d")
        h3 = FileHeader(name="other.file", description="d")
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "a.b.c.d.md")
        kb_db.insert_file(h3, "other.file.md")
        # Entry links to a.b.c.d (NOT a.b)
        kb_db.insert_entry("other.file", 100, "See [[a.b.c.d]] for more")
        links = kb_db.get_links_for_file("a.b")
        assert "other.file" not in links["inbound_entries"]

    def test_inbound_entries_exact_match(self, kb_db):
        """[[a.b]] should match as inbound for a.b."""
        h1 = FileHeader(name="a.b", description="d")
        h2 = FileHeader(name="other.file", description="d")
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "other.file.md")
        kb_db.insert_entry("other.file", 100, "See [[a.b]] for more")
        links = kb_db.get_links_for_file("a.b")
        assert "other.file" in links["inbound_entries"]

    def test_inbound_entries_epoch_link_match(self, kb_db):
        """[[a.b#12345]] should match as inbound for a.b."""
        h1 = FileHeader(name="a.b", description="d")
        h2 = FileHeader(name="other.file", description="d")
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "other.file.md")
        kb_db.insert_entry("other.file", 100, "See [[a.b#12345]] for details")
        links = kb_db.get_links_for_file("a.b")
        assert "other.file" in links["inbound_entries"]


class TestOutboundEntries:
    def test_outbound_entries_present(self, kb_db):
        """File-level links response includes outbound_entries."""
        h1 = FileHeader(name="a.b", description="d", links=[])
        h2 = FileHeader(name="c.d", description="d", links=[])
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "c.d.md")
        kb_db.insert_entry("a.b", 100, "See [[c.d]] for more")
        links = kb_db.get_links_for_file("a.b")
        assert "outbound_entries" in links
        assert "c.d" in links["outbound_entries"]

    def test_outbound_entries_excludes_self(self, kb_db):
        """Self-references are excluded from outbound_entries."""
        h = FileHeader(name="a.b", description="d", links=[])
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry("a.b", 100, "See [[a.b]] for self")
        links = kb_db.get_links_for_file("a.b")
        assert "a.b" not in links["outbound_entries"]

    def test_outbound_entries_empty_no_links(self, kb_db):
        """File with no wiki-links has empty outbound_entries."""
        h = FileHeader(name="a.b", description="d", links=[])
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry("a.b", 100, "No links here.")
        links = kb_db.get_links_for_file("a.b")
        assert links["outbound_entries"] == []


class TestHealthQueries:
    def test_get_orphan_entry_links(self, kb_db):
        """Orphan links are entry_links whose source entry was deleted."""
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry("a.b", 100, "content")
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),
            ("a.b", 999, "c.d", 0, "file"),  # epoch 999 doesn't exist
        ])
        orphans = kb_db.get_orphan_entry_links()
        assert len(orphans) == 1
        assert orphans[0]["source_epoch"] == 999

    def test_get_all_entry_link_targets(self, kb_db):
        """Returns distinct target files with counts."""
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),
            ("a.b", 200, "c.d", 0, "file"),
            ("a.b", 100, "e.f", 0, "file"),
        ])
        targets = kb_db.get_all_entry_link_targets()
        by_name = {t["target_file"]: t["link_count"] for t in targets}
        assert by_name["c.d"] == 2
        assert by_name["e.f"] == 1

    def test_get_self_referencing_entry_links(self, kb_db):
        """Detects links where source_file == target_file."""
        kb_db.insert_entry_links([
            ("a.b", 100, "a.b", 0, "file"),  # self-link
            ("a.b", 100, "c.d", 0, "file"),  # normal
        ])
        self_links = kb_db.get_self_referencing_entry_links()
        assert len(self_links) == 1
        assert self_links[0]["source_file"] == "a.b"
        assert self_links[0]["target_file"] == "a.b"

    def test_get_misplaced_archived_entries(self, kb_db):
        """Finds archived entries in non-archive files."""
        h1 = FileHeader(name="a.b", description="d")
        h2 = FileHeader(name="a.b.archive", description="d")
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "a.b.archive.md")
        kb_db.insert_entry("a.b", 100, "misplaced", status="archived")
        kb_db.insert_entry("a.b.archive", 200, "correct", status="archived")
        misplaced = kb_db.get_misplaced_archived_entries()
        assert len(misplaced) == 1
        assert misplaced[0]["file_name"] == "a.b"

    def test_get_entry_count_by_file(self, kb_db):
        """Returns per-file entry counts."""
        h1 = FileHeader(name="a.b", description="d")
        h2 = FileHeader(name="c.d", description="d")
        kb_db.insert_file(h1, "a.b.md")
        kb_db.insert_file(h2, "c.d.md")
        kb_db.insert_entry("a.b", 100, "e1")
        kb_db.insert_entry("a.b", 200, "e2")
        kb_db.insert_entry("c.d", 300, "e3")
        counts = kb_db.get_entry_count_by_file()
        by_name = {c["file_name"]: c["entry_count"] for c in counts}
        assert by_name["a.b"] == 2
        assert by_name["c.d"] == 1

    def test_delete_orphan_entry_links(self, kb_db):
        """Deletes orphan rows and returns count."""
        h = FileHeader(name="a.b", description="d")
        kb_db.insert_file(h, "a.b.md")
        kb_db.insert_entry("a.b", 100, "content")
        kb_db.insert_entry_links([
            ("a.b", 100, "c.d", 0, "file"),  # valid
            ("a.b", 999, "c.d", 0, "file"),  # orphan
            ("x.y", 888, "c.d", 0, "file"),  # orphan
        ])
        deleted = kb_db.delete_orphan_entry_links()
        assert deleted == 2
        # Only valid link remains
        refs = kb_db.get_entry_references("a.b", 100)
        assert len(refs) == 1
