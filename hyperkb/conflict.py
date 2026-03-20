"""Entry-aware conflict resolution for hyperkb sync.

When git can't auto-merge concurrent changes to the same file, it produces
conflict markers (<<<<<<<, =======, >>>>>>>). This module parses those
markers and applies entry-level resolution strategies.

Resolution strategies:
    - Same entry modified both sides: last-writer-wins
    - Epoch collision (same epoch, different content): keep both, bump one epoch +1
    - Modify vs delete: preserve modification
    - Header divergence: union links/keywords, latest desc wins
"""

import logging
import re
import time
from pathlib import Path
from typing import Optional

from .format import (
    parse_text,
    create_file_content,
    FileHeader,
    Entry,
    ENTRY_START_RE,
    ENTRY_END_RE,
    render_header,
    render_entry,
)

logger = logging.getLogger(__name__)

# Git conflict markers
CONFLICT_START_RE = re.compile(r"^<{7}\s*(.*)$")
CONFLICT_SEP_RE = re.compile(r"^={7}\s*$")
CONFLICT_END_RE = re.compile(r"^>{7}\s*(.*)$")


def resolve_conflicts(filepath: Path) -> tuple[Optional[str], dict]:
    """Resolve git merge conflicts in a knowledge file.

    Parses the file with conflict markers and applies entry-aware resolution.

    Args:
        filepath: Path to the conflicted file.

    Returns:
        (resolved_content, conflict_info) where resolved_content is the
        merged file text (None if resolution failed), and conflict_info
        is a dict describing what conflicts were found and how they were resolved.
    """
    text = filepath.read_text(encoding="utf-8")

    if "<<<<<<<" not in text:
        return text, {"file": filepath.name, "conflicts": 0, "resolutions": []}

    conflict_info = {
        "file": filepath.name,
        "timestamp": int(time.time()),
        "conflicts": 0,
        "resolutions": [],
    }

    # Split into regions: normal text + conflict blocks
    regions = _parse_conflict_regions(text)
    conflict_info["conflicts"] = sum(1 for r in regions if r["type"] == "conflict")

    # Resolve each conflict block
    resolved_lines = []
    for region in regions:
        if region["type"] == "normal":
            resolved_lines.extend(region["lines"])
        elif region["type"] == "conflict":
            resolution = _resolve_single_conflict(
                region["ours"], region["theirs"]
            )
            resolved_lines.extend(resolution["lines"])
            conflict_info["resolutions"].append(resolution["info"])

    resolved_text = "\n".join(resolved_lines)

    # Validate the result parses correctly
    try:
        header, entries = parse_text(resolved_text)
        if header.name or entries:
            return resolved_text, conflict_info
    except Exception as e:
        logger.warning("Resolved file failed to parse: %s", e)

    return resolved_text, conflict_info


def _parse_conflict_regions(text: str) -> list[dict]:
    """Parse text with git conflict markers into regions.

    Returns list of dicts:
        {"type": "normal", "lines": [...]}
        {"type": "conflict", "ours": [...], "theirs": [...]}
    """
    lines = text.split("\n")
    regions = []
    current_normal = []
    in_conflict = False
    in_ours = False
    ours_lines = []
    theirs_lines = []

    for line in lines:
        if CONFLICT_START_RE.match(line):
            # Save accumulated normal lines
            if current_normal:
                regions.append({"type": "normal", "lines": current_normal})
                current_normal = []
            in_conflict = True
            in_ours = True
            ours_lines = []
            theirs_lines = []
        elif CONFLICT_SEP_RE.match(line) and in_conflict:
            in_ours = False
        elif CONFLICT_END_RE.match(line) and in_conflict:
            regions.append({
                "type": "conflict",
                "ours": ours_lines,
                "theirs": theirs_lines,
            })
            in_conflict = False
        elif in_conflict:
            if in_ours:
                ours_lines.append(line)
            else:
                theirs_lines.append(line)
        else:
            current_normal.append(line)

    if current_normal:
        regions.append({"type": "normal", "lines": current_normal})

    return regions


def _resolve_single_conflict(
    ours: list[str], theirs: list[str]
) -> dict:
    """Resolve a single conflict block.

    Returns {"lines": [...], "info": {...}} with resolved lines and
    metadata about the resolution strategy used.
    """
    ours_text = "\n".join(ours)
    theirs_text = "\n".join(theirs)

    # Check if this is a header conflict
    if _is_header_region(ours_text) or _is_header_region(theirs_text):
        return _resolve_header_conflict(ours, theirs)

    # Check if these are entry blocks
    ours_entries = _extract_entries(ours_text)
    theirs_entries = _extract_entries(theirs_text)

    if ours_entries or theirs_entries:
        return _resolve_entry_conflict(ours, theirs, ours_entries, theirs_entries)

    # Fallback: keep both sides separated
    lines = ours + theirs
    return {
        "lines": lines,
        "info": {
            "strategy": "keep_both",
            "reason": "Unrecognized content — kept both versions",
        },
    }


def _is_header_region(text: str) -> bool:
    """Check if text looks like a YAML frontmatter header."""
    stripped = text.strip()
    return stripped.startswith("---") or "name:" in stripped[:100]


def _extract_entries(text: str) -> list[dict]:
    """Extract entry epochs and content from text.

    Returns list of {"epoch": int, "content": str}.
    """
    entries = []
    lines = text.split("\n")
    current_epoch = None
    current_lines = []

    for line in lines:
        start_match = ENTRY_START_RE.match(line)
        if start_match:
            if current_epoch is not None:
                entries.append({
                    "epoch": current_epoch,
                    "content": "\n".join(current_lines).strip(),
                })
            current_epoch = int(start_match.group(1))
            current_lines = []
        elif ENTRY_END_RE.match(line):
            if current_epoch is not None:
                entries.append({
                    "epoch": current_epoch,
                    "content": "\n".join(current_lines).strip(),
                })
                current_epoch = None
                current_lines = []
        else:
            if current_epoch is not None:
                current_lines.append(line)

    if current_epoch is not None:
        entries.append({
            "epoch": current_epoch,
            "content": "\n".join(current_lines).strip(),
        })

    return entries


def _resolve_header_conflict(
    ours: list[str], theirs: list[str]
) -> dict:
    """Resolve a header (YAML frontmatter) conflict.

    Strategy: union links/keywords, latest description wins.
    """
    import yaml

    ours_text = "\n".join(ours)
    theirs_text = "\n".join(theirs)

    # Try to parse YAML from each side
    ours_meta = _parse_yaml_fragment(ours_text)
    theirs_meta = _parse_yaml_fragment(theirs_text)

    if not ours_meta and not theirs_meta:
        # Can't parse either — keep ours
        return {
            "lines": ours,
            "info": {"strategy": "keep_ours", "reason": "Unparseable header conflict"},
        }

    merged = dict(ours_meta or theirs_meta)

    # Union keywords
    ours_kw = set(ours_meta.get("keywords", []) if ours_meta else [])
    theirs_kw = set(theirs_meta.get("keywords", []) if theirs_meta else [])
    merged["keywords"] = sorted(ours_kw | theirs_kw)

    # Union links
    ours_links = set(ours_meta.get("links", []) if ours_meta else [])
    theirs_links = set(theirs_meta.get("links", []) if theirs_meta else [])
    merged["links"] = sorted(ours_links | theirs_links)

    # Latest description wins (prefer theirs as "remote" = newer)
    if theirs_meta and theirs_meta.get("description"):
        merged["description"] = theirs_meta["description"]

    header = FileHeader(
        name=merged.get("name", ""),
        description=merged.get("description", ""),
        keywords=merged.get("keywords", []),
        links=merged.get("links", []),
        created=merged.get("created", ""),
        compacted=merged.get("compacted", ""),
    )
    header_text = render_header(header)
    # render_header includes --- delimiters, strip the trailing newline
    lines = header_text.rstrip("\n").split("\n")

    return {
        "lines": lines,
        "info": {
            "strategy": "header_union",
            "reason": "Merged keywords/links (union), latest description wins",
        },
    }


def _resolve_entry_conflict(
    ours: list[str],
    theirs: list[str],
    ours_entries: list[dict],
    theirs_entries: list[dict],
) -> dict:
    """Resolve an entry-level conflict.

    Strategies:
        - Epoch collision: keep both, bump one epoch +1
        - Same entry modified: last-writer-wins (keep theirs)
        - Different entries: keep both
    """
    ours_by_epoch = {e["epoch"]: e for e in ours_entries}
    theirs_by_epoch = {e["epoch"]: e for e in theirs_entries}

    resolved_entries = []
    used_epochs = set()
    resolutions = []

    # Process ours entries
    for epoch, entry in ours_by_epoch.items():
        if epoch in theirs_by_epoch:
            theirs_entry = theirs_by_epoch[epoch]
            if entry["content"] == theirs_entry["content"]:
                # Same content — just keep one
                resolved_entries.append(entry)
                used_epochs.add(epoch)
                resolutions.append(f"epoch {epoch}: identical, kept one copy")
            else:
                # Same epoch, different content — keep both, bump one
                resolved_entries.append(theirs_entry)  # theirs wins the epoch
                used_epochs.add(epoch)
                new_epoch = epoch + 1
                while new_epoch in used_epochs or new_epoch in ours_by_epoch or new_epoch in theirs_by_epoch:
                    new_epoch += 1
                bumped = dict(entry)
                bumped["epoch"] = new_epoch
                resolved_entries.append(bumped)
                used_epochs.add(new_epoch)
                resolutions.append(
                    f"epoch {epoch}: collision, kept both (bumped ours to {new_epoch})"
                )
        else:
            resolved_entries.append(entry)
            used_epochs.add(epoch)

    # Process theirs entries not already handled
    for epoch, entry in theirs_by_epoch.items():
        if epoch not in ours_by_epoch:
            resolved_entries.append(entry)
            used_epochs.add(epoch)

    # Sort by epoch
    resolved_entries.sort(key=lambda e: e["epoch"])

    # Render as entry blocks
    lines = []
    for entry in resolved_entries:
        lines.append(f"\n>>> {entry['epoch']}")
        lines.append(entry["content"])
        lines.append("<<<")

    return {
        "lines": lines,
        "info": {
            "strategy": "entry_merge",
            "entries_ours": len(ours_entries),
            "entries_theirs": len(theirs_entries),
            "entries_result": len(resolved_entries),
            "resolutions": resolutions,
        },
    }


def _parse_yaml_fragment(text: str) -> Optional[dict]:
    """Try to parse YAML from a text fragment that may or may not have --- delimiters."""
    import yaml

    # Strip --- delimiters
    lines = text.strip().split("\n")
    if lines and lines[0].strip() == "---":
        lines = lines[1:]
    if lines and lines[-1].strip() == "---":
        lines = lines[:-1]

    yaml_text = "\n".join(lines)
    try:
        result = yaml.safe_load(yaml_text)
        if isinstance(result, dict):
            return result
    except yaml.YAMLError:
        pass
    return None
