"""Tests for hyperkb conflict module — entry-aware merge conflict resolution."""

import pytest
from pathlib import Path

from hyperkb.conflict import (
    resolve_conflicts,
    _parse_conflict_regions,
    _resolve_single_conflict,
    _resolve_header_conflict,
    _resolve_entry_conflict,
    _extract_entries,
    _is_header_region,
    _parse_yaml_fragment,
)


# ---------------------------------------------------------------------------
# Region parsing
# ---------------------------------------------------------------------------

class TestParseConflictRegions:
    def test_no_conflicts(self):
        text = "line 1\nline 2\nline 3"
        regions = _parse_conflict_regions(text)
        assert len(regions) == 1
        assert regions[0]["type"] == "normal"
        assert regions[0]["lines"] == ["line 1", "line 2", "line 3"]

    def test_single_conflict(self):
        text = (
            "before\n"
            "<<<<<<< HEAD\n"
            "ours line\n"
            "=======\n"
            "theirs line\n"
            ">>>>>>> branch\n"
            "after"
        )
        regions = _parse_conflict_regions(text)
        assert len(regions) == 3
        assert regions[0]["type"] == "normal"
        assert regions[0]["lines"] == ["before"]
        assert regions[1]["type"] == "conflict"
        assert regions[1]["ours"] == ["ours line"]
        assert regions[1]["theirs"] == ["theirs line"]
        assert regions[2]["type"] == "normal"
        assert regions[2]["lines"] == ["after"]

    def test_multiple_conflicts(self):
        text = (
            "<<<<<<< HEAD\n"
            "a\n"
            "=======\n"
            "b\n"
            ">>>>>>> branch\n"
            "middle\n"
            "<<<<<<< HEAD\n"
            "c\n"
            "=======\n"
            "d\n"
            ">>>>>>> branch"
        )
        regions = _parse_conflict_regions(text)
        conflicts = [r for r in regions if r["type"] == "conflict"]
        assert len(conflicts) == 2

    def test_multiline_conflict(self):
        text = (
            "<<<<<<< HEAD\n"
            "line 1\n"
            "line 2\n"
            "line 3\n"
            "=======\n"
            "alt 1\n"
            "alt 2\n"
            ">>>>>>> branch"
        )
        regions = _parse_conflict_regions(text)
        conflict = [r for r in regions if r["type"] == "conflict"][0]
        assert conflict["ours"] == ["line 1", "line 2", "line 3"]
        assert conflict["theirs"] == ["alt 1", "alt 2"]


# ---------------------------------------------------------------------------
# Entry extraction
# ---------------------------------------------------------------------------

class TestExtractEntries:
    def test_single_entry(self):
        text = ">>> 100\nhello world\n<<<"
        entries = _extract_entries(text)
        assert len(entries) == 1
        assert entries[0]["epoch"] == 100
        assert entries[0]["content"] == "hello world"

    def test_multiple_entries(self):
        text = ">>> 100\nfirst\n<<<\n\n>>> 200\nsecond\n<<<"
        entries = _extract_entries(text)
        assert len(entries) == 2
        assert entries[0]["epoch"] == 100
        assert entries[1]["epoch"] == 200

    def test_no_entries(self):
        text = "just plain text"
        entries = _extract_entries(text)
        assert entries == []

    def test_entry_with_metadata(self):
        text = ">>> 100\n@status: active\n@type: note\nContent here\n<<<"
        entries = _extract_entries(text)
        assert len(entries) == 1
        assert "@status: active" in entries[0]["content"]


# ---------------------------------------------------------------------------
# Header detection and resolution
# ---------------------------------------------------------------------------

class TestHeaderDetection:
    def test_header_with_dashes(self):
        assert _is_header_region("---\nname: test\n---")

    def test_header_with_name(self):
        assert _is_header_region("name: test.file\ndescription: test")

    def test_not_header(self):
        assert not _is_header_region(">>> 100\nsome entry content\n<<<")


class TestParseYamlFragment:
    def test_with_delimiters(self):
        text = "---\nname: test\ndescription: hello\n---"
        result = _parse_yaml_fragment(text)
        assert result["name"] == "test"
        assert result["description"] == "hello"

    def test_without_delimiters(self):
        text = "name: test\nkeywords: [a, b, c]"
        result = _parse_yaml_fragment(text)
        assert result["name"] == "test"
        assert result["keywords"] == ["a", "b", "c"]

    def test_invalid_yaml(self):
        text = "this: is: not: valid: yaml: {{"
        result = _parse_yaml_fragment(text)
        # Should return None or a dict depending on YAML parser
        # The important thing is no exception
        assert result is None or isinstance(result, dict)

    def test_non_dict_yaml(self):
        text = "- just\n- a\n- list"
        result = _parse_yaml_fragment(text)
        assert result is None


class TestHeaderConflictResolution:
    def test_union_keywords(self):
        ours = [
            "name: test.file",
            "description: Our description",
            "keywords: [alpha, beta]",
            "links: [a.b]",
        ]
        theirs = [
            "name: test.file",
            "description: Their description",
            "keywords: [beta, gamma]",
            "links: [c.d]",
        ]
        result = _resolve_header_conflict(ours, theirs)
        text = "\n".join(result["lines"])
        assert result["info"]["strategy"] == "header_union"

        # Should have union of keywords
        assert "alpha" in text
        assert "beta" in text
        assert "gamma" in text

    def test_latest_description_wins(self):
        ours = ["name: test", "description: old desc", "keywords: []", "links: []"]
        theirs = ["name: test", "description: new desc", "keywords: []", "links: []"]
        result = _resolve_header_conflict(ours, theirs)
        text = "\n".join(result["lines"])
        assert "new desc" in text


# ---------------------------------------------------------------------------
# Entry conflict resolution
# ---------------------------------------------------------------------------

class TestEntryConflictResolution:
    def test_different_entries_kept(self):
        ours = [">>> 100", "entry from A", "<<<"]
        theirs = [">>> 200", "entry from B", "<<<"]
        ours_entries = [{"epoch": 100, "content": "entry from A"}]
        theirs_entries = [{"epoch": 200, "content": "entry from B"}]

        result = _resolve_entry_conflict(ours, theirs, ours_entries, theirs_entries)
        text = "\n".join(result["lines"])
        assert "entry from A" in text
        assert "entry from B" in text
        assert result["info"]["entries_result"] == 2

    def test_epoch_collision_both_kept(self):
        ours_entries = [{"epoch": 100, "content": "version A"}]
        theirs_entries = [{"epoch": 100, "content": "version B"}]

        result = _resolve_entry_conflict(
            [">>> 100", "version A", "<<<"],
            [">>> 100", "version B", "<<<"],
            ours_entries,
            theirs_entries,
        )
        text = "\n".join(result["lines"])
        # Both versions should be present
        assert "version A" in text
        assert "version B" in text
        assert result["info"]["entries_result"] == 2
        # One should have been bumped
        assert "collision" in str(result["info"]["resolutions"])

    def test_identical_entries_deduped(self):
        ours_entries = [{"epoch": 100, "content": "same content"}]
        theirs_entries = [{"epoch": 100, "content": "same content"}]

        result = _resolve_entry_conflict(
            [">>> 100", "same content", "<<<"],
            [">>> 100", "same content", "<<<"],
            ours_entries,
            theirs_entries,
        )
        assert result["info"]["entries_result"] == 1

    def test_entries_sorted_by_epoch(self):
        ours_entries = [{"epoch": 300, "content": "third"}]
        theirs_entries = [{"epoch": 100, "content": "first"}, {"epoch": 200, "content": "second"}]

        result = _resolve_entry_conflict(
            [">>> 300", "third", "<<<"],
            [">>> 100", "first", "<<<", ">>> 200", "second", "<<<"],
            ours_entries,
            theirs_entries,
        )
        lines = result["lines"]
        # Find epoch positions
        epoch_positions = [(i, line) for i, line in enumerate(lines) if line.startswith(">>> ")]
        epochs = [int(line.split()[1]) for _, line in epoch_positions]
        assert epochs == sorted(epochs)


# ---------------------------------------------------------------------------
# Full file resolution
# ---------------------------------------------------------------------------

class TestResolveConflicts:
    def test_no_conflicts_passthrough(self, tmp_path):
        f = tmp_path / "clean.md"
        f.write_text("---\nname: clean\n---\n\n>>> 100\nhello\n<<<\n")

        resolved, info = resolve_conflicts(f)
        assert resolved == f.read_text()
        assert info["conflicts"] == 0

    def test_entry_conflict_resolution(self, tmp_path):
        f = tmp_path / "conflict.md"
        f.write_text(
            "---\nname: test\ndescription: test\nkeywords: []\nlinks: []\ncreated: ''\ncompacted: ''\n---\n"
            "\n>>> 100\nexisting entry\n<<<\n"
            "<<<<<<< HEAD\n"
            "\n>>> 200\nentry from ours\n<<<\n"
            "=======\n"
            "\n>>> 300\nentry from theirs\n<<<\n"
            ">>>>>>> branch\n"
        )

        resolved, info = resolve_conflicts(f)
        assert resolved is not None
        assert info["conflicts"] == 1
        assert "entry from ours" in resolved
        assert "entry from theirs" in resolved

    def test_header_conflict_resolution(self, tmp_path):
        f = tmp_path / "header-conflict.md"
        f.write_text(
            "<<<<<<< HEAD\n"
            "---\n"
            "name: test\n"
            "description: our desc\n"
            "keywords: [alpha]\n"
            "links: []\n"
            "created: ''\n"
            "compacted: ''\n"
            "---\n"
            "=======\n"
            "---\n"
            "name: test\n"
            "description: their desc\n"
            "keywords: [beta]\n"
            "links: []\n"
            "created: ''\n"
            "compacted: ''\n"
            "---\n"
            ">>>>>>> branch\n"
            "\n>>> 100\nsome entry\n<<<\n"
        )

        resolved, info = resolve_conflicts(f)
        assert resolved is not None
        assert info["conflicts"] == 1
        # Union of keywords
        assert "alpha" in resolved
        assert "beta" in resolved

    def test_multiple_conflicts(self, tmp_path):
        f = tmp_path / "multi.md"
        f.write_text(
            "---\nname: test\ndescription: d\nkeywords: []\nlinks: []\ncreated: ''\ncompacted: ''\n---\n"
            "<<<<<<< HEAD\n"
            "\n>>> 100\nours 1\n<<<\n"
            "=======\n"
            "\n>>> 200\ntheirs 1\n<<<\n"
            ">>>>>>> branch\n"
            "\n>>> 300\nshared entry\n<<<\n"
            "<<<<<<< HEAD\n"
            "\n>>> 400\nours 2\n<<<\n"
            "=======\n"
            "\n>>> 500\ntheirs 2\n<<<\n"
            ">>>>>>> branch\n"
        )

        resolved, info = resolve_conflicts(f)
        assert info["conflicts"] == 2
        assert len(info["resolutions"]) == 2

    def test_conflict_info_has_timestamp(self, tmp_path):
        f = tmp_path / "conflict.md"
        f.write_text(
            "<<<<<<< HEAD\nours\n=======\ntheirs\n>>>>>>> branch\n"
        )
        _, info = resolve_conflicts(f)
        assert "timestamp" in info
        assert info["timestamp"] > 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_conflict_sides(self):
        result = _resolve_single_conflict([], [])
        assert "lines" in result
        assert "info" in result

    def test_ours_empty_theirs_has_content(self):
        theirs = [">>> 100", "content", "<<<"]
        result = _resolve_single_conflict([], theirs)
        text = "\n".join(result["lines"])
        assert "content" in text

    def test_resolve_nonexistent_file(self, tmp_path):
        """Passing a file that doesn't exist should raise."""
        with pytest.raises(FileNotFoundError):
            resolve_conflicts(tmp_path / "nonexistent.md")

    def test_fallback_keep_both(self):
        """Non-entry, non-header content uses keep_both strategy."""
        ours = ["some random text", "more text"]
        theirs = ["different text"]
        result = _resolve_single_conflict(ours, theirs)
        assert result["info"]["strategy"] == "keep_both"
        text = "\n".join(result["lines"])
        assert "some random text" in text
        assert "different text" in text
