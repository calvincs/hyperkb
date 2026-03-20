"""Core knowledge store: orchestrates files, database, and search.

Entries are indexed in FTS5 for BM25 search. File routing uses BM25 on
file metadata + filename matching.
"""

import json
import logging
import os
import re
import socket
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from .config import KBConfig
from .db import KBDatabase, TimeoutLock
from .format import (
    FileHeader, Entry, parse_file, create_file_content,
    append_entry_to_file, make_epoch, validate_filename, extract_wikilinks,
    safe_parse_json_list, extract_metadata, render_metadata, is_archive_file,
    VALID_STATUS, VALID_WEIGHT,
)
from .models import SearchResult, FileCandidate
from .search import HybridSearch


def _build_entry_links(source_file: str, source_epoch: int, content: str) -> list[tuple]:
    """Convert wiki-links in content to entry_links tuples.

    Returns list of (source_file, source_epoch, target_file, target_epoch, link_type).
    """
    links = extract_wikilinks(content)
    result = []
    for target_name, target_epoch in links:
        if target_epoch is None:
            result.append((source_file, source_epoch, target_name, 0, "file"))
        elif target_epoch == -1:
            result.append((source_file, source_epoch, target_name, -1, "latest"))
        else:
            result.append((source_file, source_epoch, target_name, target_epoch, "entry"))
    return result


class KnowledgeStore:
    """Main interface to the knowledge base."""

    def __init__(self, config: KBConfig):
        self.config = config
        self.root = Path(config.root)
        self.db = KBDatabase(config)
        self._search: Optional[HybridSearch] = None
        self._write_lock = TimeoutLock(timeout=60, name="store-write")
        self._git = None  # Lazy-init GitRepo for sync

    def _sync_commit(self, files: list[str], message: str) -> None:
        """Auto-commit files via git if sync is enabled. Best-effort, never raises."""
        if not self.config.sync_enabled:
            return
        try:
            if self._git is None:
                from .sync import GitRepo
                self._git = GitRepo(self.storage_dir)
                if not self._git.is_initialized():
                    self._git.init()
            self._git.auto_commit(files, message)
        except Exception as e:
            logger.debug("Sync auto-commit failed (non-fatal): %s", e)

    @property
    def storage_dir(self) -> Path:
        return self.config.storage_dir

    @property
    def search_engine(self) -> HybridSearch:
        if self._search is None:
            self._search = HybridSearch(self.config, self.db)
        return self._search

    def _get_author(self) -> str:
        """Get the author identifier for new entries.

        Priority: HKB_SOURCE env var > config.default_source > "unknown".
        """
        return os.environ.get("HKB_SOURCE", "") or self.config.default_source or "unknown"

    def init(self) -> str:
        """Initialize a new knowledge base."""
        self.root.mkdir(parents=True, exist_ok=True)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.config.root = str(self.root)
        self.config.save()
        self.db.connect()
        self.db.init_schema()
        return f"Knowledge base initialized at {self.root}"

    def close(self):
        self.db.close()

    # --- File operations ---

    def create_file(
        self,
        name: str,
        description: str,
        keywords: list[str],
        links: Optional[list[str]] = None,
    ) -> str:
        """Create a new knowledge file.

        Args:
            name: Dotted filename (e.g. "security.threat-intel.ioc-feeds").
            description: Specific description of what this file stores.
                         Be precise: what content belongs here and what doesn't.
            keywords: Search keywords for this file. Use specific, greppable terms.
            links: Optional list of related file names.

        Returns:
            Confirmation message with the file path.

        Raises:
            ValueError: If filename is invalid or already exists.
        """
        # Validate filename
        valid, err = validate_filename(name)
        if not valid:
            raise ValueError(f"Invalid filename: {err}")

        with self._write_lock:
            # Check for duplicates
            if self.db.file_exists(name):
                raise ValueError(
                    f"File '{name}' already exists. Use 'hkb check \"{description}\"' "
                    f"to find the right file, or choose a different name."
                )

            # Check for near-duplicates via search
            similar = self.search_engine.search_files(
                f"{name} {description}", limit=3
            )
            if similar:
                similar_names = [s["name"] for s in similar]
                # Don't block, just warn
                warning = (
                    f"Note: Similar files exist: {', '.join(similar_names)}. "
                    f"Proceeding with creation."
                )
            else:
                warning = ""

            # Build header
            now = datetime.now(timezone.utc).isoformat()
            header = FileHeader(
                name=name,
                description=description,
                keywords=keywords,
                links=links or [],
                created=now,
            )

            # Create the file
            filepath = self.storage_dir / f"{name}.md"
            filepath.write_text(create_file_content(header), encoding="utf-8")

            # Insert into database
            self.db.insert_file(header, str(filepath.relative_to(self.storage_dir)))

            msg = f"Created: {filepath.name}"
            if warning:
                msg += f"\n{warning}"

            self._sync_commit([f"{name}.md"], f"create file {name}")
            return msg

    def add_entry(
        self,
        content: str,
        file_name: Optional[str] = None,
        epoch: Optional[int] = None,
    ) -> str:
        """Add an entry to a knowledge file.

        Args:
            content: The knowledge to store. Should be self-contained and
                     meaningful on its own. Include context: what, when, why.
                     Use [[file.name]] or [[file.name#epoch]] for cross-references.
            file_name: Target file. If omitted, the system will find the best
                       match or suggest creating a new file.
            epoch: Override timestamp (default: now).

        Returns:
            Confirmation with file name and epoch, or suggestion to create a new file.
        """
        epoch = epoch or make_epoch()

        # Reject empty/whitespace-only content
        content = content.strip()
        if not content:
            raise ValueError("Entry content cannot be empty or whitespace-only.")

        # Reject content containing entry delimiter markers
        if re.search(r'^>>> \d+\s*$', content, re.MULTILINE) or \
           re.search(r'^<<<\s*$', content, re.MULTILINE):
            raise ValueError(
                "Entry content cannot contain lines matching entry delimiters "
                "(lines like '>>> 1234567890' or '<<<'). Rephrase these lines."
            )

        # Validate entry size
        content_size = len(content.encode("utf-8"))
        if content_size > self.config.max_entry_size:
            limit_mb = self.config.max_entry_size / 1_048_576
            size_mb = content_size / 1_048_576
            raise ValueError(
                f"Entry too large: {size_mb:.2f} MiB exceeds "
                f"{limit_mb:.2f} MiB limit. Split into smaller entries."
            )

        if file_name:
            # Direct routing
            if file_name.endswith(".md"):
                file_name = file_name[:-3]

            if not self.db.file_exists(file_name):
                raise ValueError(
                    f"File '{file_name}' not found. "
                    f"Use 'hkb list' to see available files, "
                    f"or 'hkb create' to create a new one."
                )

            return self._append_entry(file_name, content, epoch)

        # Auto-routing: find the best file for this content
        candidates = self.find_best_file(content)

        if not candidates:
            return (
                "NO_MATCH: No suitable file found for this content.\n"
                "Action required: Create a new file with 'hkb create'.\n"
                f"Suggested name based on content: [determine from content]\n"
                f"Content preview: {content[:100]}..."
            )

        best = candidates[0]
        if best.score >= self.config.route_confidence_threshold:
            return self._append_entry(best.name, content, epoch)

        # Low confidence — return candidates for the caller to decide
        candidate_lines = []
        for c in candidates[:3]:
            candidate_lines.append(
                f"  - {c.name} (score: {c.score:.2f}): {c.reason}"
            )
        return (
            f"LOW_CONFIDENCE: Best match is '{best.name}' "
            f"(score: {best.score:.2f}) but below threshold "
            f"({self.config.route_confidence_threshold}).\n"
            f"Candidates:\n" + "\n".join(candidate_lines) + "\n"
            f"Options:\n"
            f"  1. Use 'hkb add --to {best.name}' to force-add to best match\n"
            f"  2. Use 'hkb create' to create a new file for this content\n"
            f"  3. Re-run with more specific content"
        )

    def _append_entry(self, file_name: str, content: str, epoch: int) -> str:
        """Internal: append an entry to a file and index it.

        DB insert happens first, then file append. If the file write fails,
        the DB entry is rolled back via delete_entry_by_epoch().

        Metadata (@key: value lines at start of content) is parsed out and
        stored in separate DB columns for filtering.
        """
        # Parse metadata from content
        metadata, prose = extract_metadata(content)
        db_status = metadata.get("status", "active")
        db_type = metadata.get("type", "note")
        db_tags = metadata.get("tags", "")
        db_weight = metadata.get("weight", "normal")
        if db_weight not in VALID_WEIGHT:
            db_weight = "normal"

        # Auto-populate provenance if not explicitly set
        if "author" not in metadata:
            metadata["author"] = self._get_author()
        if "hostname" not in metadata:
            metadata["hostname"] = socket.gethostname()
        db_author = metadata.get("author", "")
        db_hostname = metadata.get("hostname", "")

        with self._write_lock:
            filepath = self.storage_dir / f"{file_name}.md"

            # Insert to DB first (handles epoch collision by incrementing)
            # Store prose content in DB (metadata lives in columns)
            _, actual_epoch = self.db.insert_entry(
                file_name, epoch, prose,
                status=db_status, entry_type=db_type, tags=db_tags,
                weight=db_weight, author=db_author, hostname=db_hostname,
            )

            # Write to file with the actual epoch used; rollback DB on failure
            # File stores the full content with metadata lines
            entry = Entry(
                epoch=actual_epoch, content=prose,
                file_name=file_name, metadata=metadata,
            )
            try:
                append_entry_to_file(filepath, entry)
            except OSError as e:
                logger.warning(
                    "File write failed for %s at epoch %d, rolling back DB entry: %s",
                    file_name, actual_epoch, e,
                )
                self.db.delete_entry_by_epoch(file_name, actual_epoch)
                raise

            # Extract and validate wiki-links
            links = extract_wikilinks(prose)
            broken = [name for name, _ in links if not self.db.file_exists(name)]

            # Insert entry_links for this entry
            entry_links = _build_entry_links(file_name, actual_epoch, prose)
            if entry_links:
                self.db.insert_entry_links(entry_links)

            msg = f"Added to {file_name} at epoch {actual_epoch}"
            if broken:
                msg += f"\nWarning: broken links: {', '.join(broken)}"

            self._sync_commit(
                [f"{file_name}.md"],
                f"add entry to {file_name} at {actual_epoch}",
            )
            return msg

    def update_entry(
        self,
        file_name: str,
        epoch: int,
        new_content: Optional[str] = None,
        set_status: Optional[str] = None,
        add_tags: Optional[str] = None,
        remove_tags: Optional[str] = None,
    ) -> dict:
        """Amend an existing entry's content and/or metadata.

        At least one of the parameters must be provided.

        Returns dict with status and details.
        Raises ValueError if entry/file not found or no changes requested.
        """
        if not any([new_content, set_status, add_tags, remove_tags]):
            raise ValueError("At least one of new_content, set_status, add_tags, or remove_tags must be provided.")

        if file_name.endswith(".md"):
            file_name = file_name[:-3]

        with self._write_lock:
            # Verify entry exists in DB
            db_entry = self.db.get_entry(file_name, epoch)
            if db_entry is None:
                raise ValueError(f"Entry not found: {file_name} at epoch {epoch}")

            filepath = self.storage_dir / f"{file_name}.md"
            if not filepath.exists():
                raise ValueError(f"File '{file_name}' not found on disk.")

            # Parse the file to find and update the entry
            header, entries = parse_file(filepath)
            target_idx = None
            for i, e in enumerate(entries):
                if e.epoch == epoch:
                    target_idx = i
                    break
            if target_idx is None:
                raise ValueError(f"Entry at epoch {epoch} not found in file '{file_name}'.")

            target = entries[target_idx]

            # Apply content update
            if new_content is not None:
                new_content = new_content.strip()
                if not new_content:
                    raise ValueError("new_content cannot be empty.")
                target.content = new_content

            # Apply metadata updates
            meta = dict(target.metadata)

            if set_status is not None:
                if set_status not in VALID_STATUS:
                    raise ValueError(f"Invalid status '{set_status}'. Valid: {sorted(VALID_STATUS)}")
                meta["status"] = set_status

            if add_tags is not None:
                existing = {t.strip() for t in meta.get("tags", "").split(",") if t.strip()}
                new = {t.strip() for t in add_tags.split(",") if t.strip()}
                merged = sorted(existing | new)
                meta["tags"] = ", ".join(merged)

            if remove_tags is not None:
                existing = {t.strip() for t in meta.get("tags", "").split(",") if t.strip()}
                to_remove = {t.strip() for t in remove_tags.split(",") if t.strip()}
                remaining = sorted(existing - to_remove)
                meta["tags"] = ", ".join(remaining)

            target.metadata = meta

            # Rewrite the file
            entries[target_idx] = target
            filepath.write_text(create_file_content(header, entries), encoding="utf-8")

            # Validate weight if present
            if "weight" in meta and meta["weight"] not in VALID_WEIGHT:
                meta["weight"] = "normal"

            # Update DB
            self.db.update_entry(
                file_name, epoch,
                content=target.content,
                status=meta.get("status"),
                entry_type=meta.get("type"),
                tags=meta.get("tags", ""),
                weight=meta.get("weight"),
                author=meta.get("author"),
                hostname=meta.get("hostname"),
            )

            # Re-extract entry_links if content changed
            if new_content is not None:
                self.db.delete_entry_links_for_source(file_name, epoch)
                entry_links = _build_entry_links(file_name, epoch, target.content)
                if entry_links:
                    self.db.insert_entry_links(entry_links)

            self._sync_commit(
                [f"{file_name}.md"],
                f"update entry in {file_name} at {epoch}",
            )
            return {"status": "ok", "file": file_name, "epoch": epoch}

    def archive_entry(self, file_name: str, epoch: int) -> dict:
        """Move an entry from a source file to its .archive companion file.

        Archives physically remove the entry from the source file and append
        it to the archive file with @status: archived metadata.

        Returns dict with status and archive file name.
        Raises ValueError if entry/file not found.
        """
        if file_name.endswith(".md"):
            file_name = file_name[:-3]

        archive_name = f"{file_name}.archive"

        with self._write_lock:
            # Parse source file
            filepath = self.storage_dir / f"{file_name}.md"
            if not filepath.exists():
                raise ValueError(f"File '{file_name}' not found.")

            header, entries = parse_file(filepath)
            target_idx = None
            for i, e in enumerate(entries):
                if e.epoch == epoch:
                    target_idx = i
                    break
            if target_idx is None:
                raise ValueError(f"Entry at epoch {epoch} not found in file '{file_name}'.")

            target = entries.pop(target_idx)
            target.metadata["status"] = "archived"

            # Create archive file if it doesn't exist
            archive_path = self.storage_dir / f"{archive_name}.md"
            if not archive_path.exists():
                now = datetime.now(timezone.utc).isoformat()
                archive_header = FileHeader(
                    name=archive_name,
                    description=f"Archived entries from {file_name}",
                    keywords=header.keywords,
                    links=[file_name],
                    created=now,
                )
                archive_path.write_text(
                    create_file_content(archive_header), encoding="utf-8"
                )
                self.db.insert_file(
                    archive_header,
                    str(archive_path.relative_to(self.storage_dir)),
                )

            # Append entry to archive file
            target.file_name = archive_name
            append_entry_to_file(archive_path, target)

            # Rewrite source file without the archived entry
            filepath.write_text(create_file_content(header, entries), encoding="utf-8")

            # Update DB: delete from source, insert into archive
            self.db.delete_entry_by_epoch(file_name, epoch)
            self.db.insert_entry(
                archive_name, epoch, target.content,
                status="archived",
                entry_type=target.metadata.get("type", "note"),
                tags=target.metadata.get("tags", ""),
            )

            self._sync_commit(
                [f"{file_name}.md", f"{archive_name}.md"],
                f"archive entry from {file_name} at {epoch}",
            )
            return {"status": "ok", "archived_to": archive_name, "epoch": epoch}

    @staticmethod
    def _cluster_entries(
        entries: list[Entry],
        gap_seconds: int,
        min_cluster: int,
        min_age_seconds: int,
    ) -> tuple[list[list[Entry]], list[dict], list[int]]:
        """Cluster entries by temporal gap and evaluate eligibility.

        Returns (clusters, cluster_info, eligible_indices).
        Pure function — no side effects.
        """
        SKIP_STATUS = {"archived", "superseded", "resolved"}
        compactable = [
            e for e in entries
            if e.metadata.get("status", "active") not in SKIP_STATUS
        ]
        compactable.sort(key=lambda e: e.epoch)

        clusters: list[list[Entry]] = []
        if compactable:
            current: list[Entry] = [compactable[0]]
            for i in range(1, len(compactable)):
                if compactable[i].epoch - compactable[i - 1].epoch > gap_seconds:
                    clusters.append(current)
                    current = [compactable[i]]
                else:
                    current.append(compactable[i])
            clusters.append(current)

        now = int(time.time())
        cutoff = now - min_age_seconds

        cluster_info = []
        eligible_indices = []
        for idx, cluster in enumerate(clusters):
            oldest_epoch = min(e.epoch for e in cluster)
            newest_epoch = max(e.epoch for e in cluster)
            eligible = len(cluster) >= min_cluster and newest_epoch < cutoff

            cluster_info.append({
                "size": len(cluster),
                "time_range": [
                    datetime.fromtimestamp(oldest_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
                    datetime.fromtimestamp(newest_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
                ],
                "epochs": [e.epoch for e in cluster],
                "eligible": eligible,
                "preview": " | ".join(e.content[:60] for e in cluster[:3]),
            })
            if eligible:
                eligible_indices.append(idx)

        return clusters, cluster_info, eligible_indices

    def compact_file(
        self,
        file_name: str,
        gap_seconds: int = 14400,
        min_cluster: int = 3,
        min_age_seconds: int = 604800,
        dry_run: bool = True,
    ) -> dict:
        """Compact a file by clustering temporally-related entries.

        Identifies clusters of entries written close together in time,
        replaces each cluster with a single concatenated summary entry,
        and archives the originals to the .archive companion file.

        Args:
            file_name: Dotted filename to compact.
            gap_seconds: Gap (seconds) that separates clusters (default: 4h).
            min_cluster: Minimum entries per eligible cluster (default: 3).
            min_age_seconds: Newest entry in cluster must be older than this (default: 7d).
            dry_run: If True, return analysis without side effects.

        Returns:
            Dict with status and compaction details.
        """
        if file_name.endswith(".md"):
            file_name = file_name[:-3]

        if is_archive_file(file_name):
            raise ValueError(f"Cannot compact archive file '{file_name}'.")

        filepath = self.storage_dir / f"{file_name}.md"
        if not filepath.exists():
            raise FileNotFoundError(f"File '{file_name}' not found.")

        # --- Dry-run path: read-only, no lock needed ---
        if dry_run:
            header, entries = parse_file(filepath)
            if not entries:
                return {
                    "status": "preview",
                    "file": file_name,
                    "total_entries": 0,
                    "clusters": [],
                    "eligible_clusters": 0,
                    "total_entries_to_compact": 0,
                }
            clusters, cluster_info, eligible_indices = self._cluster_entries(
                entries, gap_seconds, min_cluster, min_age_seconds,
            )
            total_to_compact = sum(len(clusters[i]) for i in eligible_indices)
            return {
                "status": "preview",
                "file": file_name,
                "total_entries": len(entries),
                "clusters": cluster_info,
                "eligible_clusters": len(eligible_indices),
                "total_entries_to_compact": total_to_compact,
            }

        # --- Execution path: parse inside lock for consistency ---
        with self._write_lock:
            header, entries = parse_file(filepath)

            if not entries:
                return {
                    "status": "ok",
                    "file": file_name,
                    "compacted_clusters": 0,
                    "entries_archived": 0,
                    "summaries_created": 0,
                    "entries_remaining": 0,
                }

            clusters, _, eligible_indices = self._cluster_entries(
                entries, gap_seconds, min_cluster, min_age_seconds,
            )

            if not eligible_indices:
                return {
                    "status": "ok",
                    "file": file_name,
                    "compacted_clusters": 0,
                    "entries_archived": 0,
                    "summaries_created": 0,
                    "entries_remaining": len(entries),
                }

            epochs_to_archive: set[int] = set()
            summary_entries: list[Entry] = []
            archive_entries: list[Entry] = []

            for idx in eligible_indices:
                cluster = clusters[idx]
                cluster_epochs = [e.epoch for e in cluster]
                epochs_to_archive.update(cluster_epochs)

                oldest_epoch = cluster_epochs[0]
                newest_epoch = cluster_epochs[-1]
                oldest_ts = datetime.fromtimestamp(oldest_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
                newest_ts = datetime.fromtimestamp(newest_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ")

                # Build concatenated summary content
                parts = [f"[Compacted {len(cluster)} entries from {oldest_ts} to {newest_ts}]", ""]
                for i, e in enumerate(cluster):
                    parts.append(e.content)
                    if i < len(cluster) - 1:
                        parts.append("---")
                parts.append("")
                parts.append(f"[archived: {', '.join(str(ep) for ep in cluster_epochs)}]")
                summary_content = "\n".join(parts)

                # Union of tags
                all_tags: set[str] = set()
                for e in cluster:
                    for t in e.metadata.get("tags", "").split(","):
                        t = t.strip()
                        if t:
                            all_tags.add(t)

                summary_meta: dict[str, str] = {"type": "note"}
                if all_tags:
                    summary_meta["tags"] = ", ".join(sorted(all_tags))

                summary_entries.append(Entry(
                    epoch=oldest_epoch,
                    content=summary_content,
                    file_name=file_name,
                    metadata=summary_meta,
                ))

                # Prepare entries for archival
                for e in cluster:
                    archive_entries.append(Entry(
                        epoch=e.epoch,
                        content=e.content,
                        file_name=f"{file_name}.archive",
                        metadata={**e.metadata, "status": "archived"},
                    ))

            # Build new source file entries: replace clusters with summaries
            summary_by_epoch = {se.epoch: se for se in summary_entries}
            inserted: set[int] = set()
            new_entries: list[Entry] = []
            for e in entries:
                if e.epoch in epochs_to_archive:
                    if e.epoch in summary_by_epoch and e.epoch not in inserted:
                        new_entries.append(summary_by_epoch[e.epoch])
                        inserted.add(e.epoch)
                    continue
                new_entries.append(e)

            # Ensure archive file exists
            archive_name = f"{file_name}.archive"
            archive_path = self.storage_dir / f"{archive_name}.md"
            if not archive_path.exists():
                now_iso = datetime.now(timezone.utc).isoformat()
                archive_header = FileHeader(
                    name=archive_name,
                    description=f"Archived entries from {file_name}",
                    keywords=header.keywords,
                    links=[file_name],
                    created=now_iso,
                )
                archive_path.write_text(
                    create_file_content(archive_header), encoding="utf-8"
                )
                self.db.insert_file(
                    archive_header,
                    str(archive_path.relative_to(self.storage_dir)),
                )

            # Before bulk-append, collect existing archive epochs for dedup
            existing_archive_epochs: set[int] = set()
            if archive_path.exists():
                _, existing_arch_entries = parse_file(archive_path)
                existing_archive_epochs = {e.epoch for e in existing_arch_entries}

            # Bulk-append archived entries, skipping duplicates
            actually_archived = 0
            for ae in archive_entries:
                if ae.epoch not in existing_archive_epochs:
                    append_entry_to_file(archive_path, ae)
                    actually_archived += 1

            # Update header and rewrite source file
            header.compacted = datetime.now(timezone.utc).isoformat()
            filepath.write_text(
                create_file_content(header, new_entries), encoding="utf-8"
            )

            # Update DB: remove originals, add archived + summaries
            for epoch in epochs_to_archive:
                self.db.delete_entry_by_epoch(file_name, epoch)

            for ae in archive_entries:
                if ae.epoch not in existing_archive_epochs:
                    self.db.insert_entry(
                        archive_name, ae.epoch, ae.content,
                        status="archived",
                        entry_type=ae.metadata.get("type", "note"),
                        tags=ae.metadata.get("tags", ""),
                    )

            for se in summary_entries:
                self.db.insert_entry(
                    file_name, se.epoch, se.content,
                    status="active",
                    entry_type=se.metadata.get("type", "note"),
                    tags=se.metadata.get("tags", ""),
                )

            self._sync_commit(
                [f"{file_name}.md", f"{file_name}.archive.md"],
                f"compact {file_name}: {len(eligible_indices)} clusters",
            )
            return {
                "status": "ok",
                "file": file_name,
                "compacted_clusters": len(eligible_indices),
                "entries_archived": actually_archived,
                "summaries_created": len(summary_entries),
                "entries_remaining": len(new_entries),
            }

    def get_recent(
        self,
        after_epoch: int = 0,
        before_epoch: int = 0,
        limit: int = 20,
        domain: str = "",
        author: str = "",
        hostname: str = "",
    ) -> list[dict]:
        """Get recent entries across all files, sorted by newest first."""
        return self.db.get_recent_entries(
            after_epoch=after_epoch,
            before_epoch=before_epoch,
            limit=limit,
            domain=domain,
            author=author,
            hostname=hostname,
        )

    def find_best_file(self, content: str, limit: int = 5) -> list[FileCandidate]:
        """Find the best file(s) for a piece of content.

        Uses hybrid search on file metadata to score candidates.
        """
        candidates = []
        file_results = self.search_engine.search_files(content, limit=limit)

        for f in file_results:
            name = f.get("name", "")
            desc = f.get("description", "")
            kw = safe_parse_json_list(f.get("keywords", "[]"))
            score = f.get("_score", 0.0)

            candidates.append(FileCandidate(
                name=name,
                description=desc,
                keywords=kw,
                score=score,
                reason=f"Matched via {f.get('_source', 'unknown')}",
            ))

        return sorted(candidates, key=lambda c: c.score, reverse=True)

    # --- Query operations ---

    def search(
        self,
        query: str,
        mode: str = "hybrid",
        limit: int = 10,
        domain: Optional[str] = None,
        after_epoch: Optional[int] = None,
        before_epoch: Optional[int] = None,
        offset: int = 0,
        status: Optional[str] = None,
        entry_type: Optional[str] = None,
        include_archived: bool = False,
        author: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> list[SearchResult]:
        """Search the knowledge base.

        Args:
            query: Search terms. Use specific keywords or phrases for best results.
                   For exact matches, use mode="rg".
                   For keyword ranking, use mode="bm25".
            mode: Search method.
                  "hybrid" - combines rg + bm25 (default, best for general use)
                  "rg"     - ripgrep exact/regex match (fastest, most precise)
                  "bm25"   - SQLite FTS5 ranked keyword search
            limit: Maximum results to return (default 10).
            domain: Scope search to a domain prefix (e.g. "security", "projects").
            after_epoch: Only return entries after this epoch.
            before_epoch: Only return entries before this epoch.
            offset: Number of results to skip (for pagination).
            status: Filter by entry status (e.g. "active", "superseded").
            entry_type: Filter by entry type (e.g. "note", "task", "decision").
            include_archived: If True, include entries from .archive files.
            author: Filter by entry author (AI client name).
            hostname: Filter by entry hostname (machine name).

        Returns:
            List of SearchResult with file_name, content, epoch, score, source.
        """
        return self.search_engine.search(
            query, mode=mode, limit=limit, domain=domain,
            after_epoch=after_epoch, before_epoch=before_epoch,
            offset=offset, status=status, entry_type=entry_type,
            exclude_archives=not include_archived,
            author=author, hostname=hostname,
        )

    def show_file(
        self,
        name: str,
        after_epoch: Optional[int] = None,
        before_epoch: Optional[int] = None,
        last_n: Optional[int] = None,
    ) -> dict:
        """Show a file's metadata and entries.

        Args:
            name: The dotted filename (with or without .md).
            after_epoch: Only show entries after this epoch.
            before_epoch: Only show entries before this epoch.
            last_n: Only show the N most recent entries.

        Returns:
            Dict with 'header' and 'entries' keys.
        """
        if name.endswith(".md"):
            name = name[:-3]

        filepath = self.storage_dir / f"{name}.md"
        if not filepath.exists():
            raise FileNotFoundError(f"File '{name}' not found.")

        header, entries = parse_file(filepath)

        if after_epoch is not None:
            entries = [e for e in entries if e.epoch > after_epoch]
        if before_epoch is not None:
            entries = [e for e in entries if e.epoch < before_epoch]
        if last_n is not None:
            entries = entries[-last_n:]

        return {
            "header": header.to_dict(),
            "entries": [{"epoch": e.epoch, "content": e.content} for e in entries],
        }

    def list_files(self, domain: Optional[str] = None) -> list[dict]:
        """List all knowledge files, optionally filtered by domain prefix.

        Args:
            domain: Filter to files starting with this prefix (e.g. "security").

        Returns:
            List of dicts with file metadata.
        """
        return self.db.list_files(domain)

    def get_links(self, name: str) -> dict:
        """Get the link graph for a file.

        Shows outbound links (from header), inbound links (from other files'
        headers), and inbound entry links (wiki-links in other files' entries).
        """
        if name.endswith(".md"):
            name = name[:-3]
        return self.db.get_links_for_file(name)

    def get_entry_links(self, file_name: str, epoch: int) -> dict:
        """Get the link graph for a specific entry.

        Returns outbound references and inbound backlinks from the
        computed entry_links table (populated during reindex).
        """
        if file_name.endswith(".md"):
            file_name = file_name[:-3]
        outbound = self.db.get_entry_references(file_name, epoch)
        inbound = self.db.get_entry_backlinks(file_name, epoch)
        return {"outbound": outbound, "inbound": inbound}

    def check_content(self, content: str) -> list[FileCandidate]:
        """Check which file(s) content would be routed to without actually adding it.

        Useful for previewing routing decisions before committing.

        Args:
            content: The content to classify.

        Returns:
            Ranked list of candidate files with scores and reasons.
        """
        return self.find_best_file(content, limit=5)

    # --- Context retrieval methods ---

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """Rough token estimate: ~4 chars per token."""
        return len(text) // 4

    @staticmethod
    def _type_priority(entry_type: str, status: str) -> float:
        """Priority multiplier for context packing by entry type."""
        type_mult = {
            "decision": 1.5, "finding": 1.3, "milestone": 1.2,
            "task": 1.1, "skill": 1.2, "note": 1.0,
        }
        status_mult = {"pending": 1.1, "in_progress": 1.1, "blocked": 1.05}
        return type_mult.get(entry_type, 1.0) * status_mult.get(status, 1.0)

    def build_context(
        self,
        topic: str,
        max_tokens: int = 4000,
        domain: str = "",
        depth: str = "deep",
    ) -> dict:
        """Search for a topic and pack the most relevant entries into a token budget.

        Args:
            topic: Search query.
            max_tokens: Token budget (default 4000).
            domain: Optional domain filter.
            depth: "deep" (full content) or "shallow" (first 200 chars per entry).

        Returns:
            Dict with topic, tokens_used, tokens_budget, depth, entries, truncated, file_summaries.
        """
        results = self.search(topic, limit=30, domain=domain or None)

        # Re-rank by score * type priority
        for r in results:
            r.score *= self._type_priority(r.entry_type, r.status)
        results.sort(key=lambda r: r.score, reverse=True)

        # Reserve tokens for file summaries
        summary_reserve = 200
        entry_budget = max_tokens - summary_reserve

        entries_out = []
        truncated = []
        tokens_used = 0
        files_seen = set()

        for r in results:
            content = r.content
            if depth == "shallow":
                content = content[:200]

            entry_tokens = self._estimate_tokens(content)
            if tokens_used + entry_tokens > entry_budget:
                truncated.append({
                    "file_name": r.file_name,
                    "epoch": r.epoch,
                    "score": round(r.score, 4),
                    "tokens": entry_tokens,
                })
                continue

            entries_out.append({
                "file_name": r.file_name,
                "epoch": r.epoch,
                "content": content,
                "score": round(r.score, 4),
                "type": r.entry_type,
                "status": r.status,
                "weight": r.weight,
            })
            tokens_used += entry_tokens
            files_seen.add(r.file_name)

        # File summaries for referenced files
        file_summaries = []
        for fname in sorted(files_seen):
            fdata = self.db.get_file(fname)
            if fdata:
                desc = fdata.get("description", "")
                file_summaries.append({
                    "name": fname,
                    "description": desc[:200],
                })

        summary_tokens = sum(self._estimate_tokens(fs["description"]) for fs in file_summaries)
        tokens_used += summary_tokens

        return {
            "topic": topic,
            "tokens_used": tokens_used,
            "tokens_budget": max_tokens,
            "depth": depth,
            "entries": entries_out,
            "truncated": truncated,
            "file_summaries": file_summaries,
        }

    def suggest_context(self, task: str, top: int = 5) -> dict:
        """Suggest files the AI should read before starting a task.

        Uses search + link graph expansion to surface relevant files.

        Args:
            task: Description of the task.
            top: Max files to suggest.

        Returns:
            Dict with task and suggestions list.
        """
        results = self.search(task, limit=top * 3)

        # Collect file-level data: best score per file + reasons
        file_scores: dict[str, float] = {}
        file_reasons: dict[str, str] = {}
        file_entry_counts: dict[str, int] = {}

        for r in results:
            fn = r.file_name
            file_entry_counts[fn] = file_entry_counts.get(fn, 0) + 1
            if fn not in file_scores or r.score > file_scores[fn]:
                file_scores[fn] = r.score
                file_reasons[fn] = "direct"

        # Link graph expansion: for each result, check outbound links
        linked_files: set[str] = set()
        for r in results:
            if r.epoch:
                refs = self.db.get_entry_references(r.file_name, r.epoch)
                for ref in refs:
                    target = ref["target_file"]
                    if target not in file_scores:
                        linked_files.add(target)

        for linked in linked_files:
            if self.db.file_exists(linked):
                # Give linked files a lower score
                best_referrer_score = max(
                    (file_scores.get(r.file_name, 0) for r in results
                     if r.epoch and any(
                         ref["target_file"] == linked
                         for ref in self.db.get_entry_references(r.file_name, r.epoch)
                     )),
                    default=0.0,
                )
                file_scores[linked] = best_referrer_score * 0.6
                file_reasons[linked] = "linked"
                file_entry_counts[linked] = file_entry_counts.get(linked, 0)

        # Build suggestions sorted by score
        suggestions = []
        for fname in sorted(file_scores, key=lambda f: file_scores[f], reverse=True)[:top]:
            fdata = self.db.get_file(fname)
            desc = fdata.get("description", "") if fdata else ""
            suggestions.append({
                "file_name": fname,
                "description": desc,
                "reason": file_reasons.get(fname, "direct"),
                "relevance": round(file_scores[fname], 4),
                "score": round(file_scores[fname], 4),
                "entry_count": file_entry_counts.get(fname, 0),
            })

        return {"task": task, "suggestions": suggestions}

    def build_narrative(
        self,
        topic: str,
        chronological: bool = True,
        depth: int = 1,
        limit: int = 50,
        domain: str = "",
        after_epoch: int = 0,
        before_epoch: int = 0,
    ) -> dict:
        """Reconstruct the story of a topic by combining search with link-graph traversal.

        Args:
            topic: Search query.
            chronological: Sort by time (True) or relevance (False).
            depth: Link hops to follow (0-2, default 1).
            limit: Max seed results.
            domain: Optional domain filter.
            after_epoch: Time filter start.
            before_epoch: Time filter end.

        Returns:
            Dict with topic, timeline, files_involved, entry_count.
        """
        depth = max(0, min(depth, 2))

        seeds = self.search(
            topic, limit=limit, domain=domain or None,
            after_epoch=after_epoch if after_epoch else None,
            before_epoch=before_epoch if before_epoch else None,
        )

        # Collect unique (file_name, epoch) keys with scores and relation type
        seen: dict[tuple, dict] = {}
        for r in seeds:
            key = (r.file_name, r.epoch)
            if key not in seen:
                seen[key] = {"score": r.score, "relation": "seed"}

        # Expand via link graph
        if depth > 0:
            frontier = [(r.file_name, r.epoch) for r in seeds if r.epoch]
            for hop in range(depth):
                next_frontier = []
                for fn, ep in frontier:
                    refs = self.db.get_entry_references(fn, ep)
                    for ref in refs:
                        target_fn = ref["target_file"]
                        target_ep = ref["target_epoch"]
                        if target_ep > 0:
                            key = (target_fn, target_ep)
                            if key not in seen:
                                # Inherit a decayed score from the parent
                                parent_score = seen.get((fn, ep), {}).get("score", 0)
                                seen[key] = {
                                    "score": parent_score * 0.5,
                                    "relation": f"linked-hop{hop + 1}",
                                }
                                next_frontier.append(key)
                frontier = next_frontier

        # Fetch full entries
        keys_to_fetch = [k for k in seen if k[1] and k[1] > 0]
        entries = self.db.get_entries_by_keys(keys_to_fetch)

        # Build timeline
        timeline = []
        for e in entries:
            key = (e["file_name"], e["epoch"])
            meta = seen.get(key, {})
            timeline.append({
                "epoch": e["epoch"],
                "file_name": e["file_name"],
                "content": e["content"],
                "type": e.get("entry_type") or "note",
                "status": e.get("status") or "active",
                "relation": meta.get("relation", "seed"),
                "score": round(meta.get("score", 0), 4),
            })

        if chronological:
            timeline.sort(key=lambda t: t["epoch"])
        else:
            timeline.sort(key=lambda t: t["score"], reverse=True)

        files_involved = sorted({t["file_name"] for t in timeline})

        return {
            "topic": topic,
            "timeline": timeline,
            "files_involved": files_involved,
            "entry_count": len(timeline),
        }

    # --- View operations ---

    VIEWS_FILE = "views.workspaces"

    def set_view(self, name: str, files: list[str], description: str = "") -> dict:
        """Create or update a named context view.

        Views are stored as @type: view entries in the views.workspaces file.

        Args:
            name: View name (e.g. "auth-refactor").
            files: List of file names in this view.
            description: Optional description.

        Returns:
            Dict with status and view details.
        """
        if not name or not name.strip():
            raise ValueError("View name cannot be empty.")
        if not files:
            raise ValueError("View must include at least one file.")

        # Ensure views file exists
        if not self.db.file_exists(self.VIEWS_FILE):
            self.create_file(
                name=self.VIEWS_FILE,
                description="Named context views — file groupings for focused work.",
                keywords=["view", "workspace", "context", "focus"],
            )

        # Check if view already exists — update by archiving the old one
        existing_entries = self.db.get_entries(self.VIEWS_FILE)
        for e in existing_entries:
            content = e["content"]
            if content.startswith(f"name: {name}\n") or content == f"name: {name}":
                # Archive old view entry
                try:
                    self.archive_entry(self.VIEWS_FILE, e["epoch"])
                except Exception:
                    pass  # Not fatal if archive fails

        # Build view entry content
        content_parts = [
            "@type: view",
            "@status: active",
            f"name: {name}",
            f"files: {', '.join(files)}",
        ]
        if description:
            content_parts.append(f"description: {description}")

        content = "\n".join(content_parts)
        self.add_entry(content=content, file_name=self.VIEWS_FILE)

        return {
            "status": "ok",
            "name": name,
            "files": files,
            "description": description,
        }

    def get_view(self, name: str) -> dict | None:
        """Get a named context view by name.

        Returns:
            Dict with name, files, description, or None if not found.
        """
        if not self.db.file_exists(self.VIEWS_FILE):
            return None

        entries = self.db.get_entries(self.VIEWS_FILE)
        for e in entries:
            status = e.get("status", "active")
            if status == "archived":
                continue
            content = e["content"]
            if content.startswith(f"name: {name}\n") or content.strip() == f"name: {name}":
                return self._parse_view_entry(content)
        return None

    def list_views(self) -> list[dict]:
        """List all active views.

        Returns:
            List of view dicts with name, files, description.
        """
        if not self.db.file_exists(self.VIEWS_FILE):
            return []

        entries = self.db.get_entries(self.VIEWS_FILE)
        views = []
        for e in entries:
            status = e.get("status", "active")
            if status == "archived":
                continue
            entry_type = e.get("entry_type", "note")
            if entry_type != "view":
                continue
            view = self._parse_view_entry(e["content"])
            if view:
                views.append(view)
        return views

    @staticmethod
    def _parse_view_entry(content: str) -> dict | None:
        """Parse a view entry's content into a structured dict."""
        result = {"name": "", "files": [], "description": ""}
        for line in content.split("\n"):
            line = line.strip()
            if line.startswith("name: "):
                result["name"] = line[6:].strip()
            elif line.startswith("files: "):
                result["files"] = [f.strip() for f in line[7:].split(",") if f.strip()]
            elif line.startswith("description: "):
                result["description"] = line[13:].strip()
        if result["name"]:
            return result
        return None

    def sync_entry_links(self):
        """Populate entry_links table if it's empty but entries exist.

        Safe to call on every startup — skips if links already populated.
        Only processes entries from disk files, not a full reindex.
        """
        entry_count = self.db.count_entries()
        link_count = self.db.count_entry_links()
        if entry_count > 0 and link_count == 0:
            logger.info("Syncing entry_links for %d entries...", entry_count)
            all_links: list[tuple] = []
            for file_path in sorted(self.storage_dir.glob("*.md")):
                if ".archive" in file_path.stem:
                    continue
                try:
                    header, entries = parse_file(file_path)
                except Exception as e:
                    logger.warning("Skipping %s during entry_links sync: %s", file_path.name, e)
                    continue
                for entry in entries:
                    all_links.extend(
                        _build_entry_links(header.name, entry.epoch, entry.content)
                    )
            if all_links:
                self.db.insert_entry_links(all_links)
                logger.info("Synced %d entry_links.", len(all_links))

    # --- Health checks ---

    def health_snapshot(self) -> dict:
        """Tier 1 health checks — pure SQL, fast enough for startup caching.

        Returns a dict with check results suitable for embedding in hkb_briefing.
        """
        checks = [
            self._check_entry_links_coverage(),
            self._check_orphan_entry_links(),
            self._check_db_vs_disk_file_count(),
            self._check_empty_files(),
        ]
        return self._compile_health_result(checks)

    def health_check(self, include_tier3: bool = True, fix: bool = False) -> dict:
        """Full health check — all tiers with optional auto-fix.

        Args:
            include_tier3: Include content analysis checks (stale/untagged/duplicates).
            fix: Auto-fix safe issues (orphan links, missing entry_links).

        Returns a dict with check results, summary, and any fixes applied.
        """
        fixes_applied = []

        # Tier 1
        t1_checks = [
            self._check_entry_links_coverage(),
            self._check_orphan_entry_links(),
            self._check_db_vs_disk_file_count(),
            self._check_empty_files(),
        ]

        # Auto-fix T1 issues
        if fix:
            for c in t1_checks:
                if c["name"] == "entry_links_coverage" and c["status"] != "ok":
                    self.sync_entry_links()
                    fixes_applied.append({
                        "name": "entry_links_coverage",
                        "action": "Ran sync_entry_links()",
                    })
                elif c["name"] == "orphan_entry_links" and c["count"] > 0:
                    deleted = self.db.delete_orphan_entry_links()
                    fixes_applied.append({
                        "name": "orphan_entry_links",
                        "action": f"Deleted {deleted} orphan rows",
                    })

        # Tier 2
        t2_checks = [
            self._check_broken_outbound_links(),
            self._check_self_links(),
            self._check_header_link_integrity(),
            self._check_misplaced_archived(),
            self._check_compaction_readiness(),
            self._check_disk_db_entry_drift(),
        ]

        # Tier 3
        t3_checks = []
        if include_tier3:
            t3_checks = [
                self._check_stale_active(),
                self._check_untagged_entries(),
                self._check_potential_duplicates(),
            ]

        all_checks = t1_checks + t2_checks + t3_checks
        result = self._compile_health_result(all_checks)
        if fixes_applied:
            result["fixes_applied"] = fixes_applied
        return result

    @staticmethod
    def _compile_health_result(checks: list[dict]) -> dict:
        """Build a summary from a list of check results."""
        ok = sum(1 for c in checks if c["status"] == "ok")
        warning = sum(1 for c in checks if c["status"] == "warning")
        error = sum(1 for c in checks if c["status"] == "error")
        total_issues = sum(c["count"] for c in checks if c["status"] != "ok")
        return {
            "summary": {
                "checks_run": len(checks),
                "ok": ok,
                "warning": warning,
                "error": error,
                "total_issues": total_issues,
            },
            "checks": checks,
        }

    # --- Tier 1 checks (SQL-only) ---

    def _check_entry_links_coverage(self) -> dict:
        """Check if entry_links table is empty when entries exist."""
        entry_count = self.db.count_entries()
        link_count = self.db.count_entry_links()
        if entry_count > 0 and link_count == 0:
            return {
                "name": "entry_links_coverage",
                "status": "warning",
                "count": 1,
                "details": [{"entries": entry_count, "links": link_count}],
                "message": f"{entry_count} entries exist but entry_links table is empty.",
                "fix_hint": "Run hkb_health with fix=True to sync entry links.",
            }
        return {
            "name": "entry_links_coverage",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": f"{link_count} entry links for {entry_count} entries.",
            "fix_hint": "",
        }

    def _check_orphan_entry_links(self) -> dict:
        """Check for entry_links referencing deleted source entries."""
        orphans = self.db.get_orphan_entry_links()
        if orphans:
            return {
                "name": "orphan_entry_links",
                "status": "warning",
                "count": len(orphans),
                "details": orphans,
                "message": f"{len(orphans)} entry_link(s) reference non-existent source entries.",
                "fix_hint": "Run hkb_health with fix=True to delete orphan rows.",
            }
        return {
            "name": "orphan_entry_links",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "No orphan entry links.",
            "fix_hint": "",
        }

    def _check_db_vs_disk_file_count(self) -> dict:
        """Check file count mismatch between DB and disk."""
        db_files = {f["name"] for f in self.db.list_files()}
        disk_files = set()
        for fp in sorted(self.storage_dir.glob("*.md")):
            disk_files.add(fp.stem)

        only_disk = sorted(disk_files - db_files)
        only_db = sorted(db_files - disk_files)

        if only_disk or only_db:
            details = []
            if only_disk:
                details.append({"on_disk_only": only_disk})
            if only_db:
                details.append({"in_db_only": only_db})
            return {
                "name": "db_vs_disk_file_count",
                "status": "error" if only_db else "warning",
                "count": len(only_disk) + len(only_db),
                "details": details,
                "message": (
                    f"{len(only_disk)} file(s) on disk not in DB, "
                    f"{len(only_db)} file(s) in DB not on disk."
                ),
                "fix_hint": "Run hkb_reindex to rebuild DB from disk.",
            }
        return {
            "name": "db_vs_disk_file_count",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": f"DB and disk agree: {len(db_files)} files.",
            "fix_hint": "",
        }

    def _check_empty_files(self) -> dict:
        """Check for non-archive files with 0 entries."""
        all_files = self.db.list_files()
        empty = [
            f["name"] for f in all_files
            if (f.get("entry_count") or 0) == 0
            and not is_archive_file(f["name"])
        ]
        if empty:
            return {
                "name": "empty_files",
                "status": "warning",
                "count": len(empty),
                "details": [{"files": empty}],
                "message": f"{len(empty)} non-archive file(s) have 0 entries.",
                "fix_hint": "Add entries or remove unused files.",
            }
        return {
            "name": "empty_files",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "All non-archive files have entries.",
            "fix_hint": "",
        }

    # --- Tier 2 checks (SQL + light disk scan) ---

    def _check_broken_outbound_links(self) -> dict:
        """Check for wiki-links targeting non-existent files."""
        targets = self.db.get_all_entry_link_targets()
        broken = []
        for t in targets:
            target_name = t["target_file"]
            if not self.db.file_exists(target_name):
                broken.append({"target": target_name, "link_count": t["link_count"]})
        if broken:
            return {
                "name": "broken_outbound_links",
                "status": "warning",
                "count": len(broken),
                "details": broken,
                "message": f"{len(broken)} wiki-link target(s) point to non-existent files.",
                "fix_hint": "Create the missing files or update the wiki-links.",
            }
        return {
            "name": "broken_outbound_links",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "All wiki-link targets exist.",
            "fix_hint": "",
        }

    def _check_self_links(self) -> dict:
        """Check for entries linking to their own file."""
        self_links = self.db.get_self_referencing_entry_links()
        if self_links:
            return {
                "name": "self_links",
                "status": "warning",
                "count": len(self_links),
                "details": self_links,
                "message": f"{len(self_links)} entry link(s) reference their own file.",
                "fix_hint": "Remove self-referencing [[wiki-links]] from entry content.",
            }
        return {
            "name": "self_links",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "No self-referencing links.",
            "fix_hint": "",
        }

    def _check_header_link_integrity(self) -> dict:
        """Check that header links: fields reference existing files."""
        all_files = self.db.list_files()
        all_names = {f["name"] for f in all_files}
        broken = []
        for f in all_files:
            links = safe_parse_json_list(f.get("links", "[]"))
            for link in links:
                if link not in all_names:
                    broken.append({"file": f["name"], "broken_link": link})
        if broken:
            return {
                "name": "header_link_integrity",
                "status": "warning",
                "count": len(broken),
                "details": broken,
                "message": f"{len(broken)} header link(s) reference non-existent files.",
                "fix_hint": "Update file headers to fix broken links.",
            }
        return {
            "name": "header_link_integrity",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "All header links valid.",
            "fix_hint": "",
        }

    def _check_misplaced_archived(self) -> dict:
        """Check for @status: archived entries in non-archive files."""
        misplaced = self.db.get_misplaced_archived_entries()
        if misplaced:
            return {
                "name": "misplaced_archived",
                "status": "warning",
                "count": len(misplaced),
                "details": [
                    {"file": m["file_name"], "epoch": m["epoch"]}
                    for m in misplaced
                ],
                "message": f"{len(misplaced)} archived entry/entries in non-archive files.",
                "fix_hint": "Use hkb_archive to move them to .archive files.",
            }
        return {
            "name": "misplaced_archived",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "No misplaced archived entries.",
            "fix_hint": "",
        }

    def _check_compaction_readiness(self) -> dict:
        """Check for files with clusters eligible for compaction."""
        from .format import parse_file as _parse_file
        eligible_files = []
        for fp in sorted(self.storage_dir.glob("*.md")):
            if is_archive_file(fp.stem):
                continue
            try:
                header, entries = _parse_file(fp)
            except Exception:
                continue
            if len(entries) < 3:
                continue
            _, _, eligible_indices = self._cluster_entries(
                entries,
                gap_seconds=14400,
                min_cluster=3,
                min_age_seconds=604800,
            )
            if eligible_indices:
                eligible_files.append({
                    "file": header.name,
                    "eligible_clusters": len(eligible_indices),
                })
        if eligible_files:
            return {
                "name": "compaction_readiness",
                "status": "warning",
                "count": len(eligible_files),
                "details": eligible_files,
                "message": f"{len(eligible_files)} file(s) have clusters ready for compaction.",
                "fix_hint": "Run hkb_compact on these files.",
            }
        return {
            "name": "compaction_readiness",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "No files ready for compaction.",
            "fix_hint": "",
        }

    def _check_disk_db_entry_drift(self) -> dict:
        """Check per-file entry count mismatch between disk and DB."""
        from .format import parse_file as _parse_file
        db_counts = {
            r["file_name"]: r["entry_count"]
            for r in self.db.get_entry_count_by_file()
        }
        drifts = []
        for fp in sorted(self.storage_dir.glob("*.md")):
            name = fp.stem
            try:
                _, entries = _parse_file(fp)
            except Exception:
                continue
            disk_count = len(entries)
            db_count = db_counts.get(name, 0)
            if disk_count != db_count:
                drifts.append({
                    "file": name,
                    "disk_entries": disk_count,
                    "db_entries": db_count,
                })
        if drifts:
            return {
                "name": "disk_db_entry_drift",
                "status": "warning",
                "count": len(drifts),
                "details": drifts,
                "message": f"{len(drifts)} file(s) have entry count mismatch between disk and DB.",
                "fix_hint": "Run hkb_reindex to rebuild DB from disk.",
            }
        return {
            "name": "disk_db_entry_drift",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "Disk and DB entry counts match.",
            "fix_hint": "",
        }

    # --- Tier 3 checks (content analysis) ---

    def _check_stale_active(self) -> dict:
        """Check for active entries older than 30 days."""
        now = int(time.time())
        thirty_days = 30 * 86400
        cutoff = now - thirty_days
        all_files = self.db.list_files()
        stale = []
        for f in all_files:
            if is_archive_file(f["name"]):
                continue
            entries = self.db.get_entries(f["name"])
            for e in entries:
                status = e.get("status") or "active"
                if status == "active" and e["epoch"] < cutoff:
                    stale.append({"file": f["name"], "epoch": e["epoch"]})
        if stale:
            return {
                "name": "stale_active",
                "status": "warning",
                "count": len(stale),
                "details": stale[:50],  # cap details
                "message": f"{len(stale)} active entry/entries older than 30 days.",
                "fix_hint": "Review and update status (superseded/resolved) or add tags.",
            }
        return {
            "name": "stale_active",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "No stale active entries.",
            "fix_hint": "",
        }

    def _check_untagged_entries(self) -> dict:
        """Check for entries with no @tags."""
        all_files = self.db.list_files()
        untagged = []
        for f in all_files:
            if is_archive_file(f["name"]):
                continue
            entries = self.db.get_entries(f["name"])
            for e in entries:
                if not e.get("tags"):
                    untagged.append({"file": f["name"], "epoch": e["epoch"]})
        if untagged:
            return {
                "name": "untagged_entries",
                "status": "warning",
                "count": len(untagged),
                "details": untagged[:50],  # cap details
                "message": f"{len(untagged)} entry/entries have no tags.",
                "fix_hint": "Use hkb_update with add_tags to tag entries.",
            }
        return {
            "name": "untagged_entries",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "All entries have tags.",
            "fix_hint": "",
        }

    def _check_potential_duplicates(self) -> dict:
        """Check for same file, first 80 chars match."""
        all_files = self.db.list_files()
        duplicates = []
        for f in all_files:
            if is_archive_file(f["name"]):
                continue
            entries = self.db.get_entries(f["name"])
            seen: dict[str, list[int]] = {}
            for e in entries:
                prefix = e["content"][:80].lower().strip()
                seen.setdefault(prefix, []).append(e["epoch"])
            for prefix, epochs in seen.items():
                if len(epochs) > 1:
                    duplicates.append({
                        "file": f["name"],
                        "epochs": epochs,
                        "preview": prefix[:80],
                    })
        if duplicates:
            return {
                "name": "potential_duplicates",
                "status": "warning",
                "count": len(duplicates),
                "details": duplicates[:20],  # cap details
                "message": f"{len(duplicates)} potential duplicate group(s) found.",
                "fix_hint": "Review and archive/remove duplicates.",
            }
        return {
            "name": "potential_duplicates",
            "status": "ok",
            "count": 0,
            "details": [],
            "message": "No potential duplicates found.",
            "fix_hint": "",
        }

    # --- Reindex ---

    def reindex(self) -> str:
        """Reindex all files from disk into the database.

        Reads all .md files in the knowledge base root, parses them,
        and rebuilds the SQLite index (files + entries + FTS + vectors).

        Uses a two-pass approach: parse ALL files first, then clear and
        rebuild the DB atomically. This prevents a single corrupt file
        from destroying the index and ensures readers never see an empty
        database mid-reindex.
        """
        # Pass 1: Parse all files into memory (outside lock — read-only)
        parsed_files = []
        parse_errors = []
        for filepath in sorted(self.storage_dir.glob("*.md")):
            try:
                header, entries = parse_file(filepath)
                if not header.name:
                    header.name = filepath.stem
                parsed_files.append((filepath, header, entries))
            except Exception as e:
                logger.warning("Skipping corrupt file %s during reindex: %s", filepath.name, e)
                parse_errors.append(f"{filepath.name}: {e}")

        # Pass 2: Clear DB and re-insert everything atomically (under lock).
        # Uses BEGIN IMMEDIATE so the entire delete+insert is a single
        # transaction — readers never see an empty database.
        with self._write_lock:
            conn = self.db.connect()
            try:
                conn.execute("BEGIN IMMEDIATE")

                # Clear all tables.
                conn.execute("DELETE FROM entries")
                conn.execute("DELETE FROM files")
                for tbl in ("entries_fts", "files_fts", "entry_links"):
                    try:
                        conn.execute(f"DELETE FROM {tbl}")
                    except sqlite3.OperationalError:
                        pass  # table may not exist

                count_files = 0
                count_entries = 0
                all_links: list[tuple] = []

                for filepath, header, entries in parsed_files:
                    # Insert file (inline SQL — bypass db.insert_file which has its own lock/commit)
                    conn.execute(
                        "INSERT INTO files (name, path, description, keywords, links, "
                        "created_at, compacted_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            header.name,
                            str(filepath.relative_to(self.storage_dir)),
                            header.description,
                            json.dumps(header.keywords),
                            json.dumps(header.links),
                            header.created,
                            header.compacted,
                        ),
                    )

                    # FTS for file
                    try:
                        conn.execute(
                            "INSERT INTO files_fts(name, description, keywords) VALUES (?, ?, ?)",
                            (header.name, header.description, " ".join(header.keywords)),
                        )
                    except sqlite3.OperationalError:
                        pass

                    count_files += 1

                    for entry in entries:
                        meta = entry.metadata
                        # Insert entry — OR IGNORE handles duplicate epochs from archive files
                        cursor = conn.execute(
                            "INSERT OR IGNORE INTO entries (file_name, epoch, content, status, entry_type, tags, weight, author, hostname) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                header.name,
                                entry.epoch,
                                entry.content,
                                meta.get("status", "active"),
                                meta.get("type", "note"),
                                meta.get("tags", ""),
                                meta.get("weight", "normal"),
                                meta.get("author", ""),
                                meta.get("hostname", ""),
                            ),
                        )
                        if cursor.rowcount > 0:
                            rowid = cursor.lastrowid
                            count_entries += 1

                            # FTS for entry
                            try:
                                conn.execute(
                                    "INSERT INTO entries_fts(rowid, content, file_name) VALUES (?, ?, ?)",
                                    (rowid, entry.content, header.name),
                                )
                            except sqlite3.OperationalError:
                                pass

                            # Collect wiki-links
                            all_links.extend(
                                _build_entry_links(header.name, entry.epoch, entry.content)
                            )
                        else:
                            logger.warning(
                                "Duplicate epoch %d in %s during reindex — skipped",
                                entry.epoch, header.name,
                            )

                # Bulk-insert entry links
                if all_links:
                    conn.executemany(
                        "INSERT OR IGNORE INTO entry_links "
                        "(source_file, source_epoch, target_file, target_epoch, link_type) "
                        "VALUES (?, ?, ?, ?, ?)",
                        all_links,
                    )
                count_links = len(all_links)

                conn.commit()
            except Exception:
                conn.rollback()
                raise

        msg = f"Reindexed {count_files} files with {count_entries} entries."
        if count_links:
            msg += f" {count_links} entry link(s) indexed."
        if parse_errors:
            msg += f" Skipped {len(parse_errors)} corrupt file(s): {'; '.join(parse_errors)}"
        return msg
