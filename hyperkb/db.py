"""SQLite database layer with FTS5 for BM25 search."""

import json
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Optional

from .config import KBConfig
from .format import safe_parse_json_list, extract_wikilinks
from .models import FileHeader, Entry, SearchResult

logger = logging.getLogger(__name__)

MAX_EPOCH_RETRIES = 100
FTS5_KEYWORDS = {"AND", "OR", "NOT", "NEAR"}


class TimeoutLock:
    """A threading lock that refuses to block forever.

    Drop-in replacement for ``threading.Lock()`` in ``with`` statements.
    If the lock isn't acquired within *timeout* seconds, raises
    ``TimeoutError`` so the caller fails loudly instead of hanging.
    """

    def __init__(self, timeout: float, name: str = "lock"):
        self._lock = threading.Lock()
        self._timeout = timeout
        self._name = name

    def __enter__(self):
        if not self._lock.acquire(timeout=self._timeout):
            raise TimeoutError(
                f"Could not acquire {self._name} lock within {self._timeout}s"
            )
        return self

    def __exit__(self, *args):
        self._lock.release()


class KBDatabase:
    """Knowledge base SQLite database."""

    def __init__(self, config: KBConfig):
        self.config = config
        self.db_path = config.db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._lock = TimeoutLock(timeout=60, name="db")

    def connect(self) -> sqlite3.Connection:
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            self.conn.execute("PRAGMA foreign_keys=ON")
            # Run schema migrations on every connect so existing KBs
            # pick up new columns (e.g. status, entry_type, tags).
            self._migrate_entry_metadata(self.conn)
            self._migrate_entry_links(self.conn)
        return self.conn

    def close(self):
        if self.conn:
            try:
                # Merge WAL back into main DB and remove WAL/SHM files.
                # Prevents "database is locked" for the next process.
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.OperationalError:
                logger.debug("WAL checkpoint failed during close (non-fatal)")
            self.conn.close()
            self.conn = None

    def init_schema(self):
        """Create all tables and indexes."""
        conn = self.connect()

        conn.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                name TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                description TEXT NOT NULL,
                keywords TEXT NOT NULL DEFAULT '[]',
                links TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                compacted_at TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS entries (
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL REFERENCES files(name) ON DELETE CASCADE,
                epoch INTEGER NOT NULL,
                content TEXT NOT NULL,
                UNIQUE(file_name, epoch)
            );

            CREATE INDEX IF NOT EXISTS idx_entries_epoch ON entries(epoch);
            CREATE INDEX IF NOT EXISTS idx_entries_file ON entries(file_name);

            CREATE TABLE IF NOT EXISTS entry_links (
                source_file TEXT NOT NULL,
                source_epoch INTEGER NOT NULL,
                target_file TEXT NOT NULL,
                target_epoch INTEGER NOT NULL DEFAULT 0,
                link_type TEXT NOT NULL DEFAULT 'file',
                PRIMARY KEY (source_file, source_epoch, target_file, target_epoch, link_type)
            );
            CREATE INDEX IF NOT EXISTS idx_entry_links_target
                ON entry_links(target_file, target_epoch);
        """)

        # Schema migration: add metadata columns if missing
        self._migrate_entry_metadata(conn)

        # FTS5 for BM25 search on entries
        try:
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
                    content,
                    file_name,
                    content_rowid='rowid',
                    tokenize='porter unicode61'
                );
            """)
        except sqlite3.OperationalError:
            pass  # FTS5 not available

        # FTS5 for file metadata search (standalone, not external content)
        try:
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                    name,
                    description,
                    keywords,
                    tokenize='porter unicode61'
                );
            """)
        except sqlite3.OperationalError:
            pass

        conn.commit()

    def _migrate_entry_metadata(self, conn: sqlite3.Connection):
        """Add status, entry_type, tags columns to entries if missing.

        Safe to call before init_schema — skips if the entries table
        doesn't exist yet (it will be created with the columns by init_schema).
        """
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(entries)").fetchall()
        }
        if not existing:
            return  # Table doesn't exist yet; init_schema will handle it.
        migrations = [
            ("status", "TEXT DEFAULT 'active'"),
            ("entry_type", "TEXT DEFAULT 'note'"),
            ("tags", "TEXT DEFAULT ''"),
            ("weight", "TEXT DEFAULT 'normal'"),
            ("author", "TEXT DEFAULT ''"),
            ("hostname", "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in migrations:
            if col_name not in existing:
                conn.execute(f"ALTER TABLE entries ADD COLUMN {col_name} {col_def}")
        conn.commit()

    def _migrate_entry_links(self, conn: sqlite3.Connection):
        """Create entry_links table if missing (for existing KBs pre-dating this feature)."""
        tables = {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "entry_links" not in tables:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS entry_links (
                    source_file TEXT NOT NULL,
                    source_epoch INTEGER NOT NULL,
                    target_file TEXT NOT NULL,
                    target_epoch INTEGER NOT NULL DEFAULT 0,
                    link_type TEXT NOT NULL DEFAULT 'file',
                    PRIMARY KEY (source_file, source_epoch, target_file, target_epoch, link_type)
                );
                CREATE INDEX IF NOT EXISTS idx_entry_links_target
                    ON entry_links(target_file, target_epoch);
            """)
            conn.commit()

    # --- File operations ---

    def file_exists(self, name: str) -> bool:
        with self._lock:
            conn = self.connect()
            row = conn.execute("SELECT 1 FROM files WHERE name = ?", (name,)).fetchone()
            return row is not None

    def insert_file(self, header: FileHeader, path: str):
        with self._lock:
            conn = self.connect()

            conn.execute(
                """INSERT INTO files (name, path, description, keywords, links, created_at, compacted_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    header.name,
                    path,
                    header.description,
                    json.dumps(header.keywords),
                    json.dumps(header.links),
                    header.created,
                    header.compacted,
                )
            )

            # Index in FTS
            try:
                conn.execute(
                    "INSERT INTO files_fts(name, description, keywords) VALUES (?, ?, ?)",
                    (header.name, header.description, " ".join(header.keywords))
                )
            except sqlite3.OperationalError as e:
                logger.debug("FTS5 insert failed for file %s: %s", header.name, e)

            conn.commit()

    def get_file(self, name: str) -> Optional[dict]:
        with self._lock:
            conn = self.connect()
            row = conn.execute("SELECT * FROM files WHERE name = ?", (name,)).fetchone()
            if row:
                return dict(row)
            return None

    def list_files(self, domain_prefix: Optional[str] = None) -> list[dict]:
        with self._lock:
            conn = self.connect()
            query = (
                "SELECT f.name, f.path, f.description, f.keywords, f.links, "
                "f.created_at, f.compacted_at, "
                "COUNT(e.rowid) AS entry_count, "
                "MAX(e.epoch) AS latest_epoch "
                "FROM files f LEFT JOIN entries e ON f.name = e.file_name "
            )
            if domain_prefix:
                query += "WHERE f.name LIKE ? GROUP BY f.name ORDER BY f.name"
                rows = conn.execute(query, (f"{domain_prefix}%",)).fetchall()
            else:
                query += "GROUP BY f.name ORDER BY f.name"
                rows = conn.execute(query).fetchall()
            return [dict(r) for r in rows]

    def get_all_file_summaries(self) -> list[dict]:
        """Get name, description, keywords for all files (for routing)."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT name, description, keywords FROM files ORDER BY name"
            ).fetchall()
            return [dict(r) for r in rows]

    def update_file_metadata(
        self,
        name: str,
        description: Optional[str] = None,
        keywords: Optional[list[str]] = None,
        links: Optional[list[str]] = None,
    ) -> bool:
        """Update file metadata in files table and files_fts.

        Only updates fields that are not None. Returns False if file not found.
        """
        with self._lock:
            conn = self.connect()
            row = conn.execute("SELECT rowid, * FROM files WHERE name = ?", (name,)).fetchone()
            if not row:
                return False

            # Build UPDATE SET clause for non-None fields
            updates = {}
            if description is not None:
                updates["description"] = description
            if keywords is not None:
                updates["keywords"] = json.dumps(keywords)
            if links is not None:
                updates["links"] = json.dumps(links)

            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                conn.execute(
                    f"UPDATE files SET {set_clause} WHERE name = ?",
                    (*updates.values(), name),
                )

            # Sync FTS: delete old row, insert new
            try:
                conn.execute("DELETE FROM files_fts WHERE name = ?", (name,))
                new_desc = description if description is not None else row["description"]
                new_kw = keywords if keywords is not None else safe_parse_json_list(row["keywords"])
                kw_text = " ".join(new_kw) if isinstance(new_kw, list) else new_kw
                conn.execute(
                    "INSERT INTO files_fts(name, description, keywords) VALUES (?, ?, ?)",
                    (name, new_desc, kw_text),
                )
            except sqlite3.OperationalError as e:
                logger.debug("FTS5 sync failed for file %s: %s", name, e)

            conn.commit()
            return True

    # --- Entry operations ---

    def insert_entry(
        self,
        file_name: str,
        epoch: int,
        content: str,
        status: str = "active",
        entry_type: str = "note",
        tags: str = "",
        weight: str = "normal",
        author: str = "",
        hostname: str = "",
    ):
        with self._lock:
            conn = self.connect()

            # Handle epoch collision: increment until unique, bounded
            retries = 0
            while True:
                try:
                    cursor = conn.execute(
                        "INSERT INTO entries (file_name, epoch, content, status, entry_type, tags, weight, author, hostname) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (file_name, epoch, content, status, entry_type, tags, weight, author, hostname)
                    )
                    break
                except sqlite3.IntegrityError:
                    retries += 1
                    if retries >= MAX_EPOCH_RETRIES:
                        raise RuntimeError(
                            f"Epoch collision: exhausted {MAX_EPOCH_RETRIES} retries "
                            f"starting from epoch {epoch - retries} for file '{file_name}'"
                        )
                    epoch += 1
            rowid = cursor.lastrowid

            # Index in FTS
            try:
                conn.execute(
                    "INSERT INTO entries_fts(rowid, content, file_name) VALUES (?, ?, ?)",
                    (rowid, content, file_name)
                )
            except sqlite3.OperationalError as e:
                logger.debug("FTS5 insert failed for entry %s/%d: %s", file_name, epoch, e)

            conn.commit()
            return rowid, epoch  # Return actual epoch used (may have been incremented)

    def delete_entry_by_epoch(self, file_name: str, epoch: int) -> bool:
        """Delete an entry by file_name and epoch. Used for rollback on write failure.

        Returns True if a row was deleted, False otherwise.
        """
        with self._lock:
            conn = self.connect()
            # Get the rowid first for FTS cleanup
            row = conn.execute(
                "SELECT rowid FROM entries WHERE file_name = ? AND epoch = ?",
                (file_name, epoch)
            ).fetchone()
            if not row:
                return False

            rowid = row["rowid"]

            # Remove from FTS
            try:
                conn.execute(
                    "DELETE FROM entries_fts WHERE rowid = ?", (rowid,)
                )
            except sqlite3.OperationalError as e:
                logger.debug("FTS5 delete failed for entry rowid %d: %s", rowid, e)

            # Remove from entries table
            conn.execute(
                "DELETE FROM entries WHERE file_name = ? AND epoch = ?",
                (file_name, epoch)
            )
            conn.commit()
            return True

    def get_entry(self, file_name: str, epoch: int) -> Optional[dict]:
        """Get a single entry by file_name and epoch."""
        with self._lock:
            conn = self.connect()
            row = conn.execute(
                "SELECT * FROM entries WHERE file_name = ? AND epoch = ?",
                (file_name, epoch)
            ).fetchone()
            return dict(row) if row else None

    def update_entry(
        self,
        file_name: str,
        epoch: int,
        content: Optional[str] = None,
        status: Optional[str] = None,
        entry_type: Optional[str] = None,
        tags: Optional[str] = None,
        weight: Optional[str] = None,
        author: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> bool:
        """Update an existing entry's content and/or metadata columns.

        Returns True if updated, False if not found.
        """
        with self._lock:
            conn = self.connect()
            row = conn.execute(
                "SELECT rowid FROM entries WHERE file_name = ? AND epoch = ?",
                (file_name, epoch)
            ).fetchone()
            if not row:
                return False
            rowid = row["rowid"]

            updates = {}
            if content is not None:
                updates["content"] = content
            if status is not None:
                updates["status"] = status
            if entry_type is not None:
                updates["entry_type"] = entry_type
            if tags is not None:
                updates["tags"] = tags
            if weight is not None:
                updates["weight"] = weight
            if author is not None:
                updates["author"] = author
            if hostname is not None:
                updates["hostname"] = hostname

            if not updates:
                return True

            set_clause = ", ".join(f"{k} = ?" for k in updates)
            conn.execute(
                f"UPDATE entries SET {set_clause} WHERE file_name = ? AND epoch = ?",
                (*updates.values(), file_name, epoch),
            )

            # Update FTS if content changed
            if content is not None:
                try:
                    conn.execute("DELETE FROM entries_fts WHERE rowid = ?", (rowid,))
                    conn.execute(
                        "INSERT INTO entries_fts(rowid, content, file_name) VALUES (?, ?, ?)",
                        (rowid, content, file_name),
                    )
                except sqlite3.OperationalError as e:
                    logger.debug("FTS5 update failed for entry %s/%d: %s", file_name, epoch, e)

            conn.commit()
            return True

    def get_entries(self, file_name: str, after_epoch: Optional[int] = None,
                    last_n: Optional[int] = None) -> list[dict]:
        with self._lock:
            conn = self.connect()
            query = "SELECT * FROM entries WHERE file_name = ?"
            params: list = [file_name]

            if after_epoch is not None:
                query += " AND epoch > ?"
                params.append(after_epoch)

            query += " ORDER BY epoch ASC"

            if last_n is not None:
                # Subquery to get newest N entries in chronological order
                query = (
                    f"SELECT * FROM ({query} ) sub "
                    f"ORDER BY epoch DESC LIMIT {last_n}"
                )
                rows = conn.execute(query, params).fetchall()
                # Reverse back to chronological order
                return [dict(r) for r in reversed(rows)]

            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    # --- Search operations ---

    def bm25_search_entries(
        self,
        query: str,
        limit: int = 10,
        after_epoch: Optional[int] = None,
        before_epoch: Optional[int] = None,
        offset: int = 0,
        status: Optional[str] = None,
        entry_type: Optional[str] = None,
        exclude_archives: bool = True,
        author: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> list[SearchResult]:
        """BM25 full-text search on entries."""
        import re
        # Build FTS query outside the lock (pure computation)
        raw_terms = query.strip().split()
        terms = []
        for t in raw_terms:
            cleaned = re.sub(r'[^\w\s-]', '', t)
            if cleaned:
                if '-' in cleaned or cleaned.upper() in FTS5_KEYWORDS:
                    terms.append(f'"{cleaned}"')
                else:
                    terms.append(cleaned)
        if not terms:
            return []
        fts_query = " OR ".join(terms)

        with self._lock:
            conn = self.connect()
            results = []
            try:
                sql = """SELECT entries_fts.rowid, entries_fts.content, entries_fts.file_name,
                              rank, entries.epoch,
                              entries.status, entries.entry_type, entries.tags, entries.weight,
                              entries.author, entries.hostname
                       FROM entries_fts
                       JOIN entries ON entries.rowid = entries_fts.rowid
                       WHERE entries_fts MATCH ?"""
                params: list = [fts_query]

                if after_epoch is not None:
                    sql += " AND entries.epoch > ?"
                    params.append(after_epoch)
                if before_epoch is not None:
                    sql += " AND entries.epoch < ?"
                    params.append(before_epoch)
                if status is not None:
                    sql += " AND entries.status = ?"
                    params.append(status)
                if entry_type is not None:
                    sql += " AND entries.entry_type = ?"
                    params.append(entry_type)
                if exclude_archives:
                    sql += " AND entries.file_name NOT LIKE '%.archive'"
                if author:
                    sql += " AND entries.author = ?"
                    params.append(author)
                if hostname:
                    sql += " AND entries.hostname = ?"
                    params.append(hostname)

                sql += " ORDER BY rank LIMIT ? OFFSET ?"
                params.append(limit)
                params.append(offset)

                rows = conn.execute(sql, params).fetchall()

                for row in rows:
                    results.append(SearchResult(
                        file_name=row["file_name"],
                        content=row["content"],
                        epoch=row["epoch"],
                        score=abs(row["rank"]),  # FTS5 rank is negative
                        source="bm25",
                        snippet=row["content"][:200],
                        status=row["status"] or "",
                        entry_type=row["entry_type"] or "",
                        tags=row["tags"] or "",
                        weight=row["weight"] or "normal",
                        author=row["author"] or "",
                        hostname=row["hostname"] or "",
                    ))
            except sqlite3.OperationalError as e:
                logger.debug("BM25 entry search failed for query %r: %s", query, e)
            return results

    def get_recent_entries(
        self,
        after_epoch: int = 0,
        before_epoch: int = 0,
        limit: int = 20,
        domain: str = "",
        author: str = "",
        hostname: str = "",
    ) -> list[dict]:
        """Get recent entries across all (non-archive) files, sorted by epoch descending."""
        with self._lock:
            conn = self.connect()
            sql = """
                SELECT epoch, content, file_name, status, entry_type, tags, weight,
                       author, hostname
                FROM entries
                WHERE file_name NOT LIKE '%.archive'
                  AND (status = 'active' OR status IS NULL)
            """
            params: list = []
            if after_epoch:
                sql += " AND epoch > ?"
                params.append(after_epoch)
            if before_epoch:
                sql += " AND epoch < ?"
                params.append(before_epoch)
            if domain:
                sql += " AND file_name LIKE ?"
                params.append(f"{domain}.%")
            if author:
                sql += " AND author = ?"
                params.append(author)
            if hostname:
                sql += " AND hostname = ?"
                params.append(hostname)
            sql += " ORDER BY epoch DESC LIMIT ?"
            params.append(limit)
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def get_tasks(
        self,
        status_filter: str = "pending",
        file_name: str = "",
        domain: str = "",
    ) -> list[dict]:
        """Get task entries filtered by status, file, or domain."""
        with self._lock:
            conn = self.connect()
            sql = """
                SELECT epoch, content, file_name, status, entry_type, tags, weight,
                       author, hostname
                FROM entries
                WHERE entry_type = 'task'
            """
            params: list = []
            if status_filter and status_filter != "all":
                # Support comma-separated statuses
                statuses = [s.strip() for s in status_filter.split(",")]
                placeholders = ", ".join("?" for _ in statuses)
                sql += f" AND status IN ({placeholders})"
                params.extend(statuses)
            if file_name:
                sql += " AND file_name = ?"
                params.append(file_name)
            if domain:
                sql += " AND file_name LIKE ?"
                params.append(f"{domain}.%")
            sql += " ORDER BY epoch ASC"
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def bm25_search_files(self, query: str, limit: int = 10) -> list[dict]:
        """BM25 search on file metadata. Uses OR matching so any matching term scores."""
        import re
        raw_terms = query.strip().split()
        terms = []
        for t in raw_terms:
            cleaned = re.sub(r'[^\w\s-]', '', t)
            if cleaned:
                if '-' in cleaned or cleaned.upper() in FTS5_KEYWORDS or \
                   any(c in cleaned for c in '.*+^$'):
                    terms.append(f'"{cleaned}"')
                else:
                    terms.append(cleaned)
        if not terms:
            return []
        fts_query = " OR ".join(terms)

        with self._lock:
            conn = self.connect()
            try:
                rows = conn.execute(
                    """SELECT name, description, keywords, rank
                       FROM files_fts
                       WHERE files_fts MATCH ?
                       ORDER BY rank
                       LIMIT ?""",
                    (fts_query, limit)
                ).fetchall()
                return [dict(r) for r in rows]
            except sqlite3.OperationalError as e:
                logger.debug("BM25 file search failed for query %r: %s", query, e)
                return []

    def get_links_for_file(self, name: str) -> dict:
        """Get outbound and inbound links for a file."""
        # get_file already acquires the lock, so get that first
        file_data = self.get_file(name)
        outbound = safe_parse_json_list(file_data["links"]) if file_data else []

        with self._lock:
            conn = self.connect()

            # Inbound: files that link to this file in their header
            rows = conn.execute(
                "SELECT name, links FROM files WHERE links LIKE ?",
                (f'%"{name}"%',)
            ).fetchall()
            inbound_header = [row["name"] for row in rows if row["name"] != name]

            # Inbound from entry content (wikilinks)
            rows = conn.execute(
                "SELECT DISTINCT file_name FROM entries "
                "WHERE content LIKE ? OR content LIKE ?",
                (f"%[[{name}]]%", f"%[[{name}#%"),
            ).fetchall()
            inbound_entries = [row["file_name"] for row in rows if row["file_name"] != name]

            # Outbound from this file's entry content (wikilinks)
            rows = conn.execute(
                "SELECT content FROM entries WHERE file_name = ? AND content LIKE '%[[%'",
                (name,),
            ).fetchall()
            outbound_entries = list({
                target for row in rows
                for target, _ in extract_wikilinks(row["content"])
                if target != name
            })

        return {
            "outbound": outbound,
            "outbound_entries": sorted(outbound_entries),
            "inbound_header": inbound_header,
            "inbound_entries": list(set(inbound_entries)),
        }

    # --- Entry link operations (computed during reindex) ---

    def insert_entry_links(self, links: list[tuple]) -> int:
        """Bulk-insert entry link rows. Each tuple:
        (source_file, source_epoch, target_file, target_epoch, link_type).

        Uses INSERT OR IGNORE to silently skip duplicates.
        Returns the number of rows inserted.
        """
        if not links:
            return 0
        with self._lock:
            conn = self.connect()
            conn.executemany(
                "INSERT OR IGNORE INTO entry_links "
                "(source_file, source_epoch, target_file, target_epoch, link_type) "
                "VALUES (?, ?, ?, ?, ?)",
                links,
            )
            count = conn.total_changes
            conn.commit()
            return count

    def clear_entry_links(self):
        """Delete all rows from entry_links. Called at start of reindex."""
        with self._lock:
            conn = self.connect()
            conn.execute("DELETE FROM entry_links")
            conn.commit()

    def delete_entry_links_for_source(self, source_file: str, source_epoch: int):
        """Delete entry_links rows for a specific source entry."""
        with self._lock:
            conn = self.connect()
            conn.execute(
                "DELETE FROM entry_links WHERE source_file = ? AND source_epoch = ?",
                (source_file, source_epoch),
            )
            conn.commit()

    def count_entries(self) -> int:
        """Return total number of entries across all files."""
        with self._lock:
            return self.connect().execute("SELECT COUNT(*) FROM entries").fetchone()[0]

    def count_entry_links(self) -> int:
        """Return total number of entry_links rows."""
        with self._lock:
            return self.connect().execute("SELECT COUNT(*) FROM entry_links").fetchone()[0]

    def get_orphan_entry_links(self) -> list[dict]:
        """Find entry_links rows whose source entry no longer exists in the entries table."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT el.source_file, el.source_epoch, el.target_file, el.target_epoch, el.link_type "
                "FROM entry_links el "
                "LEFT JOIN entries e ON el.source_file = e.file_name AND el.source_epoch = e.epoch "
                "WHERE e.rowid IS NULL"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_entry_link_targets(self) -> list[dict]:
        """Get distinct target files from entry_links with counts."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT target_file, COUNT(*) AS link_count "
                "FROM entry_links GROUP BY target_file ORDER BY target_file"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_self_referencing_entry_links(self) -> list[dict]:
        """Find entry_links where source_file equals target_file."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT source_file, source_epoch, target_file, target_epoch, link_type "
                "FROM entry_links WHERE source_file = target_file"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_misplaced_archived_entries(self) -> list[dict]:
        """Find entries with status='archived' in non-archive files."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT file_name, epoch, content "
                "FROM entries "
                "WHERE status = 'archived' AND file_name NOT LIKE '%.archive'"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_entry_count_by_file(self) -> list[dict]:
        """Get entry counts grouped by file_name."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT file_name, COUNT(*) AS entry_count "
                "FROM entries GROUP BY file_name ORDER BY file_name"
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_orphan_entry_links(self) -> int:
        """Delete entry_links rows whose source entry no longer exists. Returns count deleted."""
        with self._lock:
            conn = self.connect()
            cursor = conn.execute(
                "DELETE FROM entry_links WHERE rowid IN ("
                "  SELECT el.rowid FROM entry_links el "
                "  LEFT JOIN entries e ON el.source_file = e.file_name AND el.source_epoch = e.epoch "
                "  WHERE e.rowid IS NULL"
                ")"
            )
            count = cursor.rowcount
            conn.commit()
            return count

    def get_entries_by_keys(self, keys: list[tuple]) -> list[dict]:
        """Batch-fetch entries by (file_name, epoch) pairs. Avoids N+1 queries."""
        if not keys:
            return []
        with self._lock:
            conn = self.connect()
            # Build WHERE clause with OR'd pairs
            conditions = " OR ".join(
                "(file_name = ? AND epoch = ?)" for _ in keys
            )
            params = []
            for fn, ep in keys:
                params.extend([fn, ep])
            rows = conn.execute(
                f"SELECT * FROM entries WHERE {conditions} ORDER BY epoch ASC",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_entry_references(self, file_name: str, epoch: int) -> list[dict]:
        """Get outbound links from a specific entry."""
        with self._lock:
            conn = self.connect()
            rows = conn.execute(
                "SELECT target_file, target_epoch, link_type FROM entry_links "
                "WHERE source_file = ? AND source_epoch = ? "
                "ORDER BY target_file, target_epoch",
                (file_name, epoch),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_entry_backlinks(
        self, file_name: str, epoch: Optional[int] = None
    ) -> list[dict]:
        """Get inbound links targeting a file or specific entry.

        When epoch is provided: returns links targeting that specific entry
        OR file-level links (target_epoch=0) to that file.
        When epoch is None: returns all inbound links to the file.
        """
        with self._lock:
            conn = self.connect()
            if epoch is not None:
                rows = conn.execute(
                    "SELECT source_file, source_epoch, link_type FROM entry_links "
                    "WHERE target_file = ? AND (target_epoch = ? OR target_epoch = 0) "
                    "ORDER BY source_file, source_epoch",
                    (file_name, epoch),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT source_file, source_epoch, link_type FROM entry_links "
                    "WHERE target_file = ? "
                    "ORDER BY source_file, source_epoch",
                    (file_name,),
                ).fetchall()
            return [dict(r) for r in rows]
