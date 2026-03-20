"""Tests for hyperkb.format — parsing, rendering, wikilinks, filenames, time input."""

import time
import pytest
from datetime import datetime, timezone

from hyperkb.format import (
    parse_text,
    parse_file,
    render_header,
    render_entry,
    create_file_content,
    append_entry_to_file,
    extract_wikilinks,
    validate_filename,
    make_epoch,
    parse_time_input,
    safe_parse_json_list,
    extract_metadata,
    render_metadata,
    is_archive_file,
    VALID_TYPE,
    FileHeader,
    Entry,
)


# --- parse_text / parse_file ---

class TestParseText:
    def test_empty_text(self):
        header, entries = parse_text("")
        assert header.name == ""
        assert entries == []

    def test_header_only(self):
        text = "---\nname: test.file\ndescription: A test\nkeywords: [a, b]\n---\n"
        header, entries = parse_text(text)
        assert header.name == "test.file"
        assert header.description == "A test"
        assert header.keywords == ["a", "b"]
        assert entries == []

    def test_header_and_one_entry(self):
        text = (
            "---\nname: test.file\ndescription: desc\nkeywords: []\n---\n"
            "\n>>> 1000000\nSome content here.\n<<<\n"
        )
        header, entries = parse_text(text)
        assert header.name == "test.file"
        assert len(entries) == 1
        assert entries[0].epoch == 1000000
        assert entries[0].content == "Some content here."

    def test_multiple_entries(self):
        text = (
            "---\nname: t.f\ndescription: d\n---\n"
            "\n>>> 100\nFirst entry\n<<<\n"
            "\n>>> 200\nSecond entry\nwith multiple lines\n<<<\n"
        )
        _, entries = parse_text(text)
        assert len(entries) == 2
        assert entries[0].epoch == 100
        assert entries[1].epoch == 200
        assert "multiple lines" in entries[1].content

    def test_unclosed_entry(self):
        text = "---\nname: t.f\ndescription: d\n---\n\n>>> 100\nUnclosed"
        _, entries = parse_text(text)
        assert len(entries) == 1
        assert entries[0].content == "Unclosed"

    def test_entry_file_name_from_header(self):
        text = "---\nname: my.file\ndescription: d\n---\n\n>>> 100\ncontent\n<<<\n"
        _, entries = parse_text(text)
        assert entries[0].file_name == "my.file"

    def test_malformed_yaml_returns_empty_header(self):
        """Malformed YAML should not crash; entries should still be parsed."""
        text = "---\ninvalid: [yaml: {{{\n---\n\n>>> 100\nentry content here\n<<<\n"
        header, entries = parse_text(text)
        assert header.name == ""
        assert header.description == ""
        assert len(entries) == 1
        assert entries[0].content == "entry content here"


class TestParseFile:
    def test_parse_file_derives_name(self, tmp_path):
        p = tmp_path / "domain.topic.md"
        p.write_text("---\ndescription: test\n---\n\n>>> 100\nentry\n<<<\n")
        header, entries = parse_file(p)
        assert header.name == "domain.topic"
        assert len(entries) == 1


# --- render ---

class TestRender:
    def test_render_header(self):
        h = FileHeader(name="a.b", description="desc", keywords=["x"], links=["c.d"])
        text = render_header(h)
        assert text.startswith("---\n")
        assert text.endswith("---\n")
        assert "a.b" in text
        assert "desc" in text

    def test_render_entry(self):
        e = Entry(epoch=12345, content="hello world")
        text = render_entry(e)
        assert ">>> 12345" in text
        assert "hello world" in text
        assert "<<<" in text

    def test_create_file_content(self):
        h = FileHeader(name="a.b", description="d", keywords=[], links=[])
        e = Entry(epoch=100, content="entry content")
        text = create_file_content(h, [e])
        assert "---" in text
        assert ">>> 100" in text
        assert "entry content" in text

    def test_append_entry_to_file(self, tmp_path):
        p = tmp_path / "test.md"
        p.write_text("---\nname: test.md\n---\n")
        e = Entry(epoch=999, content="appended")
        append_entry_to_file(p, e)
        content = p.read_text()
        assert ">>> 999" in content
        assert "appended" in content


# --- wikilinks ---

class TestWikilinks:
    def test_simple_link(self):
        links = extract_wikilinks("See [[file.name]] for details.")
        assert links == [("file.name", None)]

    def test_link_with_epoch(self):
        links = extract_wikilinks("Check [[file.name#1740130800]]")
        assert links == [("file.name", 1740130800)]

    def test_link_with_latest(self):
        links = extract_wikilinks("See [[file.name#latest]]")
        assert links == [("file.name", -1)]

    def test_multiple_links(self):
        text = "Link [[a.b]] and [[c.d#100]] here"
        links = extract_wikilinks(text)
        assert len(links) == 2

    def test_no_links(self):
        assert extract_wikilinks("no links here") == []

    def test_malformed_epoch(self):
        links = extract_wikilinks("[[file.name#notanumber]]")
        assert links == [("file.name", None)]


# --- validate_filename ---

class TestValidateFilename:
    def test_valid_two_segments(self):
        ok, err = validate_filename("security.findings")
        assert ok is True
        assert err == ""

    def test_valid_four_segments(self):
        ok, _ = validate_filename("a.b.c.d")
        assert ok is True

    def test_valid_with_hyphens(self):
        ok, _ = validate_filename("security.threat-intel")
        assert ok is True

    def test_valid_with_md_extension(self):
        ok, _ = validate_filename("security.findings.md")
        assert ok is True

    def test_too_few_segments(self):
        ok, err = validate_filename("security")
        assert ok is False
        assert "2 dot-separated" in err

    def test_too_many_segments(self):
        ok, err = validate_filename("a.b.c.d.e")
        assert ok is False
        assert "too many" in err.lower()

    def test_empty(self):
        ok, _ = validate_filename("")
        assert ok is False

    def test_uppercase_rejected(self):
        ok, err = validate_filename("Security.Findings")
        assert ok is False

    def test_leading_hyphen_rejected(self):
        ok, err = validate_filename("security.-findings")
        assert ok is False

    def test_consecutive_dots(self):
        ok, err = validate_filename("security..findings")
        assert ok is False

    def test_archive_five_segments_allowed(self):
        ok, _ = validate_filename("a.b.c.d.archive")
        assert ok is True

    def test_five_segments_non_archive_rejected(self):
        ok, err = validate_filename("a.b.c.d.e")
        assert ok is False

    def test_archive_three_segments(self):
        ok, _ = validate_filename("security.threat-intel.archive")
        assert ok is True


class TestIsArchiveFile:
    def test_archive_file(self):
        assert is_archive_file("security.threat-intel.archive") is True

    def test_not_archive_file(self):
        assert is_archive_file("security.threat-intel") is False

    def test_archive_with_md_extension(self):
        assert is_archive_file("security.threat-intel.archive.md") is True


# --- parse_time_input ---

class TestParseTimeInput:
    def test_raw_epoch(self):
        assert parse_time_input("1740130800") == 1740130800

    def test_relative_days(self):
        result = parse_time_input("2d")
        expected = int(time.time()) - 2 * 86400
        assert abs(result - expected) < 2  # allow 2s tolerance

    def test_relative_hours(self):
        result = parse_time_input("4h")
        expected = int(time.time()) - 4 * 3600
        assert abs(result - expected) < 2

    def test_relative_weeks(self):
        result = parse_time_input("1w")
        expected = int(time.time()) - 604800
        assert abs(result - expected) < 2

    def test_relative_minutes(self):
        result = parse_time_input("30m")
        expected = int(time.time()) - 30 * 60
        assert abs(result - expected) < 2

    def test_iso_date(self):
        result = parse_time_input("2026-02-01")
        dt = datetime(2026, 2, 1, tzinfo=timezone.utc)
        assert result == int(dt.timestamp())

    def test_iso_datetime(self):
        result = parse_time_input("2026-02-01T10:00:00")
        dt = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert result == int(dt.timestamp())

    def test_iso_datetime_with_z(self):
        result = parse_time_input("2026-02-01T10:00:00Z")
        dt = datetime(2026, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert result == int(dt.timestamp())

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_time_input("not-a-time")

    def test_whitespace_stripped(self):
        result = parse_time_input("  1740130800  ")
        assert result == 1740130800


# --- safe_parse_json_list ---

class TestSafeParseJsonList:
    def test_valid_json_list(self):
        assert safe_parse_json_list('["a", "b", "c"]') == ["a", "b", "c"]

    def test_invalid_json(self):
        assert safe_parse_json_list("not json") == []

    def test_already_list(self):
        assert safe_parse_json_list(["x", "y"]) == ["x", "y"]

    def test_non_list_json(self):
        assert safe_parse_json_list('{"key": "value"}') == []

    def test_empty_string(self):
        assert safe_parse_json_list("") == []

    def test_custom_default(self):
        assert safe_parse_json_list("bad", default=["fallback"]) == ["fallback"]

    def test_none_input(self):
        assert safe_parse_json_list(None) == []

    def test_int_input(self):
        assert safe_parse_json_list(42) == []


# --- make_epoch ---

def test_make_epoch():
    e = make_epoch()
    assert isinstance(e, int)
    assert abs(e - int(time.time())) < 2


# --- extract_metadata / render_metadata ---

class TestExtractMetadata:
    def test_no_metadata(self):
        meta, prose = extract_metadata("Just plain text here.")
        assert meta == {}
        assert prose == "Just plain text here."

    def test_single_metadata_line(self):
        meta, prose = extract_metadata("@status: active\nSome prose content.")
        assert meta == {"status": "active"}
        assert prose == "Some prose content."

    def test_multiple_metadata_lines(self):
        meta, prose = extract_metadata(
            "@type: decision\n@tags: arch, search\n@status: active\nDecision prose."
        )
        assert meta == {"type": "decision", "tags": "arch, search", "status": "active"}
        assert prose == "Decision prose."

    def test_metadata_only_no_prose(self):
        meta, prose = extract_metadata("@status: superseded\n@type: finding")
        assert meta == {"status": "superseded", "type": "finding"}
        assert prose == ""

    def test_metadata_stops_at_non_metadata_line(self):
        meta, prose = extract_metadata(
            "@type: note\nProse line.\n@tags: should-not-parse"
        )
        assert meta == {"type": "note"}
        assert "@tags: should-not-parse" in prose

    def test_empty_content(self):
        meta, prose = extract_metadata("")
        assert meta == {}
        assert prose == ""

    def test_whitespace_in_values_stripped(self):
        meta, prose = extract_metadata("@tags:   a, b, c   \nContent.")
        assert meta["tags"] == "a, b, c"

    def test_hyphenated_keys(self):
        meta, prose = extract_metadata("@my-key: my-value\nContent.")
        assert meta == {"my-key": "my-value"}
        assert prose == "Content."


class TestRenderMetadata:
    def test_empty_metadata(self):
        assert render_metadata({}) == ""

    def test_single_field(self):
        result = render_metadata({"status": "active"})
        assert result == "@status: active"

    def test_multiple_fields(self):
        result = render_metadata({"type": "decision", "tags": "a, b"})
        assert "@type: decision" in result
        assert "@tags: a, b" in result

    def test_roundtrip(self):
        original = "@type: finding\n@status: resolved\n@tags: bug, fix"
        meta, _ = extract_metadata(original)
        rendered = render_metadata(meta)
        meta2, _ = extract_metadata(rendered)
        assert meta == meta2


class TestParseTextWithMetadata:
    def test_entry_with_metadata_parsed(self):
        text = (
            "---\nname: t.f\ndescription: d\n---\n"
            "\n>>> 100\n@type: decision\n@status: active\nThe decision.\n<<<\n"
        )
        _, entries = parse_text(text)
        assert len(entries) == 1
        assert entries[0].metadata == {"type": "decision", "status": "active"}
        assert entries[0].content == "The decision."

    def test_entry_without_metadata(self):
        text = "---\nname: t.f\ndescription: d\n---\n\n>>> 100\nPlain content.\n<<<\n"
        _, entries = parse_text(text)
        assert entries[0].metadata == {}
        assert entries[0].content == "Plain content."

    def test_render_entry_with_metadata(self):
        e = Entry(epoch=100, content="Some prose", metadata={"status": "active", "type": "note"})
        text = render_entry(e)
        assert "@status: active" in text
        assert "@type: note" in text
        assert "Some prose" in text
        assert ">>> 100" in text

    def test_render_entry_without_metadata(self):
        e = Entry(epoch=100, content="No meta")
        text = render_entry(e)
        assert "@" not in text
        assert "No meta" in text

    def test_roundtrip_with_metadata(self):
        """Write entry with metadata, parse it back, verify roundtrip."""
        e = Entry(
            epoch=500, content="Roundtrip test",
            file_name="t.f",
            metadata={"type": "finding", "tags": "a, b"},
        )
        h = FileHeader(name="t.f", description="d")
        text = create_file_content(h, [e])
        _, entries = parse_text(text)
        assert len(entries) == 1
        assert entries[0].content == "Roundtrip test"
        assert entries[0].metadata == {"type": "finding", "tags": "a, b"}


class TestValidType:
    def test_skill_in_valid_type(self):
        assert "skill" in VALID_TYPE
