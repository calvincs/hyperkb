"""Markdown file format: parsing and writing for knowledge files.

File format:
    ---
    name: domain.topic.subtopic
    description: >
      What this file stores and when to add content here.
    keywords: [kw1, kw2, kw3]
    links: [other.file.name, another.file]
    created: 2026-02-21T10:00:00Z
    compacted: ""
    ---

    >>> 1740130800
    Content of the first entry goes here.
    Can span multiple lines freely.
    Links to [[other.file.name]] or [[other.file#1740130800]] allowed.
    <<<

    >>> 1740217200
    Another entry appended later.
    <<<
"""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from .models import FileHeader, Entry

ENTRY_START_RE = re.compile(r"^>>> (\d+)\s*$")
ENTRY_END_RE = re.compile(r"^<<<\s*$")
WIKILINK_RE = re.compile(r"\[\[([^\]]+)\]\]")
METADATA_RE = re.compile(r"^@([a-z0-9-]+):\s*(.*)$")

VALID_STATUS = {"active", "superseded", "resolved", "archived", "pending", "in_progress", "completed", "blocked", "cancelled"}
VALID_TYPE = {"note", "finding", "decision", "task", "milestone", "skill"}
VALID_WEIGHT = {"high", "normal", "low"}


def extract_metadata(raw_content: str) -> tuple[dict[str, str], str]:
    """Split @key: value metadata lines from prose content.

    Metadata lines must be at the very start of entry content.
    Once a non-@-metadata line is encountered, the rest is prose.

    Returns (metadata_dict, clean_content).
    """
    metadata: dict[str, str] = {}
    lines = raw_content.split("\n")
    prose_start = 0

    for i, line in enumerate(lines):
        m = METADATA_RE.match(line.strip())
        if m:
            key = m.group(1)
            value = m.group(2).strip()
            metadata[key] = value
            prose_start = i + 1
        else:
            break

    prose = "\n".join(lines[prose_start:]).strip()
    return metadata, prose


def render_metadata(metadata: dict[str, str]) -> str:
    """Render metadata as @key: value lines.

    Returns empty string if metadata is empty.
    """
    if not metadata:
        return ""
    lines = [f"@{k}: {v}" for k, v in metadata.items()]
    return "\n".join(lines)


def parse_file(filepath: Path) -> tuple[FileHeader, list[Entry]]:
    """Parse a knowledge file into its header and entries."""
    text = filepath.read_text(encoding="utf-8")
    header, entries = parse_text(text)
    if not header.name:
        # Derive name from filename
        header.name = filepath.stem
    return header, entries


def parse_text(text: str) -> tuple[FileHeader, list[Entry]]:
    """Parse raw text into header and entries."""
    lines = text.split("\n")

    # Parse YAML frontmatter
    header = FileHeader(name="", description="")
    body_start = 0
    if lines and lines[0].strip() == "---":
        end_idx = None
        for i in range(1, len(lines)):
            if lines[i].strip() == "---":
                end_idx = i
                break
        if end_idx:
            yaml_text = "\n".join(lines[1:end_idx])
            try:
                meta = yaml.safe_load(yaml_text) or {}
            except yaml.YAMLError:
                meta = {}
            header = FileHeader(
                name=meta.get("name", ""),
                description=meta.get("description", ""),
                keywords=meta.get("keywords", []),
                links=meta.get("links", []),
                created=meta.get("created", ""),
                compacted=meta.get("compacted", ""),
            )
            body_start = end_idx + 1

    # Parse entries
    entries = []
    current_epoch = None
    current_lines = []

    def _make_entry(epoch: int, raw_lines: list[str]) -> Entry:
        raw_content = "\n".join(raw_lines).strip()
        meta, prose = extract_metadata(raw_content)
        return Entry(
            epoch=epoch,
            content=prose,
            file_name=header.name,
            metadata=meta,
        )

    for line in lines[body_start:]:
        start_match = ENTRY_START_RE.match(line)
        if start_match:
            # If we were in an entry, something is wrong (missing <<<)
            # but save what we have
            if current_epoch is not None:
                entries.append(_make_entry(current_epoch, current_lines))
            current_epoch = int(start_match.group(1))
            current_lines = []
        elif ENTRY_END_RE.match(line):
            if current_epoch is not None:
                entries.append(_make_entry(current_epoch, current_lines))
                current_epoch = None
                current_lines = []
        else:
            if current_epoch is not None:
                current_lines.append(line)

    # Handle unclosed entry at end of file
    if current_epoch is not None:
        entries.append(_make_entry(current_epoch, current_lines))

    return header, entries


def render_header(header: FileHeader) -> str:
    """Render a FileHeader as YAML frontmatter."""
    meta = {
        "name": header.name,
        "description": header.description,
        "keywords": header.keywords,
        "links": header.links,
        "created": header.created,
        "compacted": header.compacted,
    }
    yaml_str = yaml.dump(meta, default_flow_style=False, sort_keys=False, allow_unicode=True)
    return f"---\n{yaml_str}---\n"


def render_entry(entry: Entry) -> str:
    """Render an Entry as a delimited block, with metadata lines before prose."""
    meta_str = render_metadata(entry.metadata)
    if meta_str:
        return f"\n>>> {entry.epoch}\n{meta_str}\n{entry.content}\n<<<\n"
    return f"\n>>> {entry.epoch}\n{entry.content}\n<<<\n"


def create_file_content(header: FileHeader, entries: Optional[list[Entry]] = None) -> str:
    """Create full file content from header and optional entries."""
    content = render_header(header)
    if entries:
        for entry in entries:
            content += render_entry(entry)
    return content


def append_entry_to_file(filepath: Path, entry: Entry) -> None:
    """Append an entry to an existing knowledge file."""
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(render_entry(entry))


def extract_wikilinks(text: str) -> list[tuple[str, Optional[int]]]:
    """Extract wiki-links from text, returning (filename, optional_epoch) pairs.

    Supports:
        [[file.name]]           -> ("file.name", None)
        [[file.name#1740130800]] -> ("file.name", 1740130800)
        [[file.name#latest]]     -> ("file.name", -1)  # -1 signals "latest"
    """
    results = []
    for match in WIKILINK_RE.finditer(text):
        link = match.group(1)
        if "#" in link:
            parts = link.split("#", 1)
            name = parts[0]
            anchor = parts[1]
            if anchor == "latest":
                results.append((name, -1))
            else:
                try:
                    results.append((name, int(anchor)))
                except ValueError:
                    results.append((name, None))
        else:
            results.append((link, None))
    return results


def make_epoch() -> int:
    """Return current epoch timestamp (seconds)."""
    return int(time.time())


DURATION_RE = re.compile(r"^(\d+)([mhdw])$")

def parse_time_input(value: str) -> int:
    """Parse a human-friendly time input into an epoch timestamp.

    Accepts:
        Raw epoch int: "1740130800"
        Relative duration: "30m", "4h", "2d", "1w"
        ISO date: "2026-02-01"
        ISO datetime: "2026-02-01T10:00:00"

    Returns:
        Integer epoch timestamp.

    Raises:
        ValueError: If the input cannot be parsed.
    """
    value = value.strip()

    # Raw epoch integer
    if value.isdigit() and len(value) >= 9:
        return int(value)

    # Relative duration: 30m, 4h, 2d, 1w
    m = DURATION_RE.match(value)
    if m:
        amount = int(m.group(1))
        unit = m.group(2)
        multipliers = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
        return int(time.time()) - (amount * multipliers[unit])

    # ISO date or datetime
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue

    raise ValueError(
        f"Cannot parse time '{value}'. Use: epoch (1740130800), "
        f"duration (2d, 4h, 1w, 30m), or ISO date (2026-02-01)."
    )


def parse_add_result(result: str) -> dict:
    """Parse add_entry's string result into structured JSON.

    Used by both CLI (--json output) and MCP server.

    Returns:
        Dict with 'status' key and contextual fields depending on result type:
        - ok: {status, file, epoch, ?broken_links}
        - no_match: {status, message}
        - low_confidence: {status, candidates[]}
    """
    if result.startswith("Added to "):
        match = re.match(r"Added to (\S+) at epoch (\d+)", result)
        if match:
            data = {
                "status": "ok",
                "file": match.group(1),
                "epoch": int(match.group(2)),
            }
            if "Warning: broken links:" in result:
                links_str = result.split("Warning: broken links: ", 1)[1]
                data["broken_links"] = [l.strip() for l in links_str.split(",")]
            return data
    elif result.startswith("NO_MATCH"):
        return {"status": "no_match", "message": result}
    elif result.startswith("LOW_CONFIDENCE"):
        data: dict = {"status": "low_confidence", "candidates": []}
        for m in re.finditer(r"  - (\S+) \(score: ([\d.]+)\): (.+)", result):
            data["candidates"].append({
                "name": m.group(1),
                "score": float(m.group(2)),
                "reason": m.group(3),
            })
        return data
    return {"status": "ok", "message": result}


def validate_filename(name: str) -> tuple[bool, str]:
    """Validate a dotted knowledge filename.

    Rules:
        - 2-4 dot-separated segments (5 allowed when last segment is "archive")
        - Each segment: lowercase alphanumeric + hyphens
        - No leading/trailing hyphens in segments
        - No consecutive dots

    Returns (is_valid, error_message).
    """
    if not name:
        return False, "Filename cannot be empty"

    # Remove .md extension if present
    if name.endswith(".md"):
        name = name[:-3]

    segments = name.split(".")
    max_segments = 5 if (segments and segments[-1] == "archive") else 4

    if len(segments) < 2:
        return False, (
            f"Filename needs at least 2 dot-separated segments (got {len(segments)}). "
            f"Format: domain.topic[.subtopic[.focus]]  Example: security.threat-intel"
        )
    if len(segments) > max_segments:
        return False, (
            f"Filename has too many segments ({len(segments)}). Max {max_segments}. "
            f"If you need more, the file scope may be too narrow. "
            f"Format: domain.topic[.subtopic[.focus]]"
        )

    segment_re = re.compile(r"^[a-z0-9][a-z0-9-]*[a-z0-9]$|^[a-z0-9]$")
    for seg in segments:
        if not seg:
            return False, "Empty segment (consecutive dots) not allowed"
        if not segment_re.match(seg):
            return False, (
                f"Segment '{seg}' invalid. Use lowercase alphanumeric + hyphens. "
                f"No leading/trailing hyphens."
            )

    return True, ""


def is_archive_file(name: str) -> bool:
    """Check if a filename is an archive file (ends with .archive)."""
    if name.endswith(".md"):
        name = name[:-3]
    return name.endswith(".archive")


def safe_parse_json_list(val, default=None) -> list:
    """Parse a value as a JSON list, handling edge cases gracefully.

    Accepts: JSON string, already-parsed list, or any value.
    Returns: list (or default if unparseable).
    """
    if default is None:
        default = []
    if isinstance(val, list):
        return val
    if not isinstance(val, str) or not val:
        return default
    try:
        parsed = json.loads(val)
        if isinstance(parsed, list):
            return parsed
        return default
    except (json.JSONDecodeError, ValueError):
        return default
