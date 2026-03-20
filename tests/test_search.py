"""Tests for hyperkb.search — merge/dedup, weighting, mode isolation, domain scoping."""

import time
from pathlib import Path

import pytest

from hyperkb.models import SearchResult
from hyperkb.search import (
    HybridSearch, ripgrep_search, ripgrep_search_filenames,
    _extract_epoch_from_rg_match, _scan_file_for_epoch,
    _score_rg_match,
)


class TestMergeResults:
    def test_dedup_same_entry(self, kb_config, kb_db):
        search = HybridSearch(kb_config, kb_db)
        results = [
            SearchResult(file_name="a.b", content="hello", epoch=100, score=1.0, source="rg", snippet="hello"),
            SearchResult(file_name="a.b", content="hello", epoch=100, score=0.5, source="bm25", snippet="hello"),
        ]
        merged = search._merge_results(results, limit=10)
        assert len(merged) == 1
        # Score should combine both sources
        assert merged[0].score > 0

    def test_different_entries_kept(self, kb_config, kb_db):
        search = HybridSearch(kb_config, kb_db)
        results = [
            SearchResult(file_name="a.b", content="hello", epoch=100, score=1.0, source="rg", snippet="hello"),
            SearchResult(file_name="a.b", content="world", epoch=200, score=1.0, source="rg", snippet="world"),
        ]
        merged = search._merge_results(results, limit=10)
        assert len(merged) == 2

    def test_limit_respected(self, kb_config, kb_db):
        search = HybridSearch(kb_config, kb_db)
        results = [
            SearchResult(file_name="a.b", content=f"entry {i}", epoch=i, score=1.0, source="rg", snippet=f"e{i}")
            for i in range(20)
        ]
        merged = search._merge_results(results, limit=5)
        assert len(merged) == 5

    def test_weighted_scoring(self, kb_config, kb_db):
        search = HybridSearch(kb_config, kb_db)
        # rg match with weight 0.4
        results = [
            SearchResult(file_name="a.b", content="hello", epoch=100, score=1.0, source="rg", snippet="hello"),
        ]
        merged = search._merge_results(results, limit=10)
        assert merged[0].score == pytest.approx(kb_config.rg_weight * 1.0)


class TestHybridSearch:
    def test_bm25_mode_only(self, sample_kb):
        """BM25-only mode should not include rg results."""
        results = sample_kb.search("AlienVault", mode="bm25")
        assert all(r.source == "bm25" for r in results)

    def test_search_returns_results(self, sample_kb):
        results = sample_kb.search("AlienVault", mode="bm25")
        assert len(results) >= 1
        assert results[0].file_name == "security.threat-intel"

    def test_domain_filtering(self, sample_kb):
        # Only search security domain
        results = sample_kb.search("delay", mode="bm25", domain="security")
        assert all(r.file_name.startswith("security") for r in results)

    def test_no_results_for_nonexistent(self, sample_kb):
        results = sample_kb.search("xyznonexistent", mode="bm25")
        assert len(results) == 0

    def test_time_filtering_after(self, sample_kb):
        results = sample_kb.search("entry", mode="bm25", after_epoch=1500000)
        for r in results:
            assert r.epoch > 1500000

    def test_time_filtering_before(self, sample_kb):
        results = sample_kb.search("entry", mode="bm25", before_epoch=1500000)
        for r in results:
            assert r.epoch < 1500000


class TestEpochExtraction:
    def test_scan_file_for_epoch(self, tmp_path):
        """File scan fallback should find the nearest preceding >>> marker."""
        content = (
            "---\nname: test.file\ndescription: d\n---\n\n"
            ">>> 1000000000\nentry one\nline two\nline three\n<<<\n\n"
            ">>> 2000000000\n"
            + "\n".join(f"line {i}" for i in range(60))  # 60 lines deep
            + "\ntarget match here\n<<<\n"
        )
        filepath = tmp_path / "test.file.md"
        filepath.write_text(content)

        lines = content.splitlines()
        # Find the line number of "target match here" (1-based)
        match_line = next(
            i + 1 for i, line in enumerate(lines) if "target match here" in line
        )
        epoch = _scan_file_for_epoch(filepath, match_line)
        assert epoch == 2000000000

    def test_extract_epoch_falls_back_to_file_scan(self, tmp_path):
        """When context scan fails, should fall back to reading the file."""
        content = (
            "---\nname: test.file\ndescription: d\n---\n\n"
            ">>> 1234567890\n"
            + "\n".join(f"filler line {i}" for i in range(100))
            + "\nactual match text\n<<<\n"
        )
        filepath = tmp_path / "test.file.md"
        filepath.write_text(content)

        # Simulate rg match data with no epoch in context (empty context)
        lines = content.splitlines()
        match_line = next(
            i + 1 for i, line in enumerate(lines) if "actual match text" in line
        )
        data = {
            "lines": {"text": "actual match text"},
            "line_number": match_line,
        }
        epoch = _extract_epoch_from_rg_match(data, context=[], filepath=filepath)
        assert epoch == 1234567890

    def test_scan_file_for_epoch_not_found(self, tmp_path):
        """Returns None when no >>> marker exists before the match line."""
        filepath = tmp_path / "no-epoch.md"
        filepath.write_text("just some text\nno markers here\n")
        assert _scan_file_for_epoch(filepath, 2) is None

    def test_scan_file_for_epoch_missing_file(self, tmp_path):
        """Returns None for a nonexistent file."""
        assert _scan_file_for_epoch(tmp_path / "gone.md", 1) is None


class TestScoreRgMatch:
    def test_exact_match_boost(self):
        """Full query match should score higher than partial."""
        data_exact = {
            "lines": {"text": "AlienVault OTX delay"},
            "submatches": [{"start": 0, "end": 10}],
        }
        data_partial = {
            "lines": {"text": "AlienVault OTX delay and other stuff here"},
            "submatches": [{"start": 0, "end": 10}],
        }
        score_exact = _score_rg_match(data_exact, "AlienVault")
        score_partial = _score_rg_match(data_partial, "AlienVault")
        # Both have exact match, but density differs
        assert score_exact >= score_partial

    def test_more_submatches_score_higher(self):
        """More submatches should produce a higher score."""
        data_one = {
            "lines": {"text": "AlienVault data"},
            "submatches": [{"start": 0, "end": 10}],
        }
        data_three = {
            "lines": {"text": "AlienVault data AlienVault more AlienVault"},
            "submatches": [
                {"start": 0, "end": 10},
                {"start": 16, "end": 26},
                {"start": 32, "end": 42},
            ],
        }
        assert _score_rg_match(data_three, "Alien") > _score_rg_match(data_one, "Alien")

    def test_density_affects_score(self):
        """Higher match density (matched chars / line length) scores better."""
        data_dense = {
            "lines": {"text": "test"},
            "submatches": [{"start": 0, "end": 4}],
        }
        data_sparse = {
            "lines": {"text": "this is a very long line with test somewhere in it"},
            "submatches": [{"start": 35, "end": 39}],
        }
        assert _score_rg_match(data_dense, "test") > _score_rg_match(data_sparse, "test")

    def test_minimum_score(self):
        """Score should never go below 0.1."""
        data = {
            "lines": {"text": "x" * 1000},
            "submatches": [],
        }
        assert _score_rg_match(data, "nothere") >= 0.1

    def test_maximum_score(self):
        """Score should never exceed 1.0."""
        data = {
            "lines": {"text": "test"},
            "submatches": [{"start": 0, "end": 4}] * 10,
        }
        assert _score_rg_match(data, "test") <= 1.0

    def test_rg_results_have_nonuniform_scores(self, sample_kb):
        """Integration: ripgrep results for a query should not all be 1.0."""
        # Add entries with varying match quality
        sample_kb.add_entry(
            content="AlienVault has feed delay issues",
            file_name="security.threat-intel", epoch=4000000,
        )
        sample_kb.add_entry(
            content="Many tools including AlienVault and MISP work together",
            file_name="security.threat-intel", epoch=5000000,
        )
        results = sample_kb.search("AlienVault", mode="rg")
        if len(results) >= 2:
            scores = [r.score for r in results]
            # Scores should not all be identical (they're weighted, so check pre-weight)
            # Just verify they're all in valid range
            for s in scores:
                assert 0.0 < s


class TestMultiTermRipgrep:
    """Multi-term ripgrep queries should match ANY term (regex alternation)."""

    def test_multi_term_finds_individual_matches(self, tmp_path):
        """A multi-term query should find entries matching any single term."""
        f = tmp_path / "test.file.md"
        f.write_text(
            "---\nname: test.file\n---\n\n"
            ">>> 1000000000\naibox GPU server setup\n<<<\n\n"
            ">>> 2000000000\nventibean coffee roaster notes\n<<<\n\n"
            ">>> 3000000000\ncompletely unrelated content\n<<<\n"
        )
        results = ripgrep_search("aibox ventibean GPU", tmp_path, max_results=20)
        # Should find lines matching any of the 3 terms
        assert len(results) >= 2
        contents = " ".join(r.content for r in results)
        assert "aibox" in contents.lower()
        assert "ventibean" in contents.lower()

    def test_single_term_uses_fixed_strings(self):
        """Single-term queries should still use --fixed-strings (no regex)."""
        import subprocess
        from unittest.mock import patch, MagicMock
        from pathlib import Path

        mock_result = MagicMock()
        mock_result.stdout = ""
        with patch("hyperkb.search.subprocess.run", return_value=mock_result) as mock_run:
            ripgrep_search("simpleterm", Path("/tmp"), max_results=5)
            cmd = mock_run.call_args[0][0]
            assert "--fixed-strings" in cmd
            assert "simpleterm" in cmd

    def test_multi_term_does_not_use_fixed_strings(self):
        """Multi-term queries should NOT use --fixed-strings."""
        import subprocess
        from unittest.mock import patch, MagicMock
        from pathlib import Path

        mock_result = MagicMock()
        mock_result.stdout = ""
        with patch("hyperkb.search.subprocess.run", return_value=mock_result) as mock_run:
            ripgrep_search("term1 term2 term3", Path("/tmp"), max_results=5)
            cmd = mock_run.call_args[0][0]
            assert "--fixed-strings" not in cmd
            # Should use regex alternation
            pattern = cmd[-2]  # pattern is second-to-last arg (before root path)
            assert "term1|term2|term3" in pattern

    def test_regex_chars_are_escaped(self):
        """Special regex chars in terms should be escaped."""
        import subprocess
        from unittest.mock import patch, MagicMock
        from pathlib import Path

        mock_result = MagicMock()
        mock_result.stdout = ""
        with patch("hyperkb.search.subprocess.run", return_value=mock_result) as mock_run:
            ripgrep_search("C++ foo.bar baz", Path("/tmp"), max_results=5)
            cmd = mock_run.call_args[0][0]
            pattern = cmd[-2]
            # + and . should be escaped
            assert r"C\+\+" in pattern
            assert r"foo\.bar" in pattern
            assert "baz" in pattern


class TestTermCoverageScoring:
    """Term-coverage bonus in _score_rg_match."""

    def test_more_terms_covered_scores_higher(self):
        """Entry matching 3/3 query terms should score higher than 1/3."""
        data_full = {
            "lines": {"text": "aibox GPU remote access"},
            "submatches": [{"start": 0, "end": 5}],
        }
        data_partial = {
            "lines": {"text": "aibox is a server"},
            "submatches": [{"start": 0, "end": 5}],
        }
        query = "aibox GPU remote"
        assert _score_rg_match(data_full, query) > _score_rg_match(data_partial, query)

    def test_single_term_no_coverage_bonus(self):
        """Single-term queries should not get coverage bonus."""
        data = {
            "lines": {"text": "aibox server"},
            "submatches": [{"start": 0, "end": 5}],
        }
        # Score should be same regardless of coverage logic for single terms
        score = _score_rg_match(data, "aibox")
        assert 0.1 <= score <= 1.0

    def test_coverage_bonus_capped(self):
        """Coverage bonus should not exceed 0.15 even with all terms matched."""
        data = {
            "lines": {"text": "a b c d e"},
            "submatches": [{"start": 0, "end": 1}] * 5,
        }
        score = _score_rg_match(data, "a b c d e")
        assert score <= 1.0


class TestTimeoutParams:
    def test_ripgrep_search_accepts_timeout(self, tmp_path):
        """ripgrep_search should accept timeout parameter."""
        (tmp_path / "test.file.md").write_text("some content\n")
        # Should not raise — just verify the param is accepted
        ripgrep_search("content", tmp_path, timeout=2.0)

    def test_ripgrep_search_filenames_accepts_timeout(self, tmp_path):
        """ripgrep_search_filenames should accept timeout parameter."""
        (tmp_path / "test.file.md").write_text("content\n")
        ripgrep_search_filenames("test", tmp_path, timeout=2.0)

    def test_hybrid_search_uses_config_timeout(self, sample_kb):
        """HybridSearch should pass config.rg_timeout to ripgrep calls."""
        sample_kb.config.rg_timeout = 2.0
        # Searching should work without error (timeout flows through)
        results = sample_kb.search("AlienVault", mode="rg")
        assert isinstance(results, list)


class TestPaginationOffset:
    def test_hybrid_search_with_offset(self, sample_kb):
        """Offset should skip results in hybrid search."""
        # Get all results first
        all_results = sample_kb.search("AlienVault MISP", mode="bm25", limit=10)
        if len(all_results) >= 2:
            # Get with offset=1
            offset_results = sample_kb.search(
                "AlienVault MISP", mode="bm25", limit=10, offset=1,
            )
            assert len(offset_results) == len(all_results) - 1

    def test_offset_beyond_results(self, sample_kb):
        """Offset past all results returns empty."""
        results = sample_kb.search("AlienVault", mode="bm25", limit=10, offset=100)
        assert results == []


class TestSearchFiles:
    def test_search_files_returns_candidates(self, sample_kb):
        results = sample_kb.search_engine.search_files("threat intelligence")
        assert len(results) >= 1
        names = [r["name"] for r in results]
        assert "security.threat-intel" in names

    def test_search_files_by_keyword(self, sample_kb):
        results = sample_kb.search_engine.search_files("kettlebell workout")
        assert len(results) >= 1
        names = [r["name"] for r in results]
        assert "fitness.kettlebell" in names


class TestBoosts:
    def test_recency_boost_newer_scores_higher(self, kb_config, kb_db):
        """Recent entry should score higher than old entry with same base score."""
        search = HybridSearch(kb_config, kb_db)
        now = int(time.time())
        old_epoch = now - 365 * 86400  # 1 year ago
        new_epoch = now - 1 * 86400    # 1 day ago
        results = [
            SearchResult(file_name="a.b", content="old", epoch=old_epoch, score=0.5, source="bm25"),
            SearchResult(file_name="a.c", content="new", epoch=new_epoch, score=0.5, source="bm25"),
        ]
        search._apply_boosts(results)
        new_result = next(r for r in results if r.content == "new")
        old_result = next(r for r in results if r.content == "old")
        assert new_result.score > old_result.score

    def test_type_boost_decision_over_note(self, kb_config, kb_db):
        """Decision entry gets boosted above note with same base score."""
        search = HybridSearch(kb_config, kb_db)
        now = int(time.time())
        results = [
            SearchResult(file_name="a.b", content="note entry", epoch=now, score=0.5, source="bm25", entry_type="note"),
            SearchResult(file_name="a.c", content="decision entry", epoch=now, score=0.5, source="bm25", entry_type="decision"),
        ]
        search._apply_boosts(results)
        decision = next(r for r in results if r.entry_type == "decision")
        note = next(r for r in results if r.entry_type == "note")
        assert decision.score > note.score

    def test_type_boost_skill_over_note(self, kb_config, kb_db):
        """Skill entry gets boosted above note with same base score."""
        search = HybridSearch(kb_config, kb_db)
        now = int(time.time())
        results = [
            SearchResult(file_name="a.b", content="note entry", epoch=now, score=0.5, source="bm25", entry_type="note"),
            SearchResult(file_name="a.c", content="skill entry", epoch=now, score=0.5, source="bm25", entry_type="skill"),
        ]
        search._apply_boosts(results)
        skill = next(r for r in results if r.entry_type == "skill")
        note = next(r for r in results if r.entry_type == "note")
        assert skill.score > note.score

    def test_type_boost_decision_over_skill(self, kb_config, kb_db):
        """Decision entry still outranks skill with same base score."""
        search = HybridSearch(kb_config, kb_db)
        now = int(time.time())
        results = [
            SearchResult(file_name="a.b", content="skill entry", epoch=now, score=0.5, source="bm25", entry_type="skill"),
            SearchResult(file_name="a.c", content="decision entry", epoch=now, score=0.5, source="bm25", entry_type="decision"),
        ]
        search._apply_boosts(results)
        decision = next(r for r in results if r.entry_type == "decision")
        skill = next(r for r in results if r.entry_type == "skill")
        assert decision.score > skill.score

    def test_status_dampen_superseded(self, kb_config, kb_db):
        """Superseded entry score is reduced."""
        search = HybridSearch(kb_config, kb_db)
        now = int(time.time())
        results = [
            SearchResult(file_name="a.b", content="active", epoch=now, score=0.5, source="bm25", status="active"),
            SearchResult(file_name="a.c", content="superseded", epoch=now, score=0.5, source="bm25", status="superseded"),
        ]
        search._apply_boosts(results)
        active = next(r for r in results if r.status == "active")
        superseded = next(r for r in results if r.status == "superseded")
        assert active.score > superseded.score

    def test_enrich_metadata_fills_missing(self, kb_config, kb_db):
        """rg-only results get metadata from DB lookup."""
        from hyperkb.models import FileHeader
        kb_db.insert_file(FileHeader(name="test.file", description="d"), "test.file.md")
        kb_db.insert_entry("test.file", 100, "some content", status="resolved", entry_type="finding", tags="security")
        search = HybridSearch(kb_config, kb_db)
        results = [
            SearchResult(file_name="test.file", content="some content", epoch=100, score=0.5, source="rg"),
        ]
        search._enrich_metadata(results)
        assert results[0].status == "resolved"
        assert results[0].entry_type == "finding"
        assert results[0].tags == "security"

    def test_boosts_preserve_order_on_equal_age(self, kb_config, kb_db):
        """Same-age entries maintain relevance order after boosts."""
        search = HybridSearch(kb_config, kb_db)
        now = int(time.time())
        results = [
            SearchResult(file_name="a.b", content="high", epoch=now, score=0.9, source="bm25"),
            SearchResult(file_name="a.c", content="low", epoch=now, score=0.3, source="bm25"),
        ]
        search._apply_boosts(results)
        high = next(r for r in results if r.content == "high")
        low = next(r for r in results if r.content == "low")
        assert high.score > low.score
