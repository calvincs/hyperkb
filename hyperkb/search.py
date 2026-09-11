"""Hybrid search: ripgrep + BM25 (FTS5) for entries, BM25 for file routing.

Entry search strategy:
  1. ripgrep: fast exact/regex match across raw .md files
  2. BM25: ranked keyword search via SQLite FTS5

File routing (search_files):
  - BM25 on file metadata + filename text match

Results are merged, deduplicated, and scored with configurable weights.
"""

import json
import logging
import math
import re
import subprocess
import shutil
import time
from pathlib import Path
from typing import Optional

from .config import KBConfig
from .db import KBDatabase
from .models import SearchResult

logger = logging.getLogger(__name__)

# Check if ripgrep is available
RG_AVAILABLE = shutil.which("rg") is not None


def ripgrep_search(
    query: str,
    root: Path,
    max_results: int = 20,
    case_sensitive: bool = False,
    regex: bool = False,
    timeout: float = 10.0,
    domain: Optional[str] = None,
) -> list[SearchResult]:
    """Search knowledge files with ripgrep.

    Parses rg JSON output to extract file, line, and match context.
    Returns results with file_name derived from the filename.
    """
    if not RG_AVAILABLE:
        return []

    cmd = [
        "rg",
        "--json",
        "--multiline",
    ]

    if domain:
        # rg's positive globs form a union. Do not include *.md here: that
        # would undo namespace pruning. Escape glob syntax to keep a literal
        # domain, matching either its own file or its dotted descendants.
        escaped_domain = re.sub(r"([\\*?{}!\[\]])", r"\\\1", domain)
        cmd.extend(["--glob", f"{escaped_domain}.md", "--glob", f"{escaped_domain}.*.md"])
    else:
        cmd.extend(["--glob", "*.md"])

    if not case_sensitive:
        cmd.append("--ignore-case")

    # Multi-term queries: use regex alternation (term1|term2|...) to match ANY term.
    # Single-term queries: keep --fixed-strings for speed.
    terms = query.split()
    if not regex and len(terms) > 1:
        rg_pattern = "|".join(re.escape(t) for t in terms)
    else:
        if not regex:
            cmd.extend(["--fixed-strings"])
        rg_pattern = query

    cmd.extend(["--", rg_pattern, str(root)])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []

    matches: list[dict] = []
    for line in result.stdout.splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") == "match":
            matches.append(obj)

    results = []
    epoch_cache = {}
    for obj in matches:
        data = obj["data"]
        match_filepath = Path(data["path"]["text"])
        file_name = match_filepath.stem
        matched_text = data["lines"]["text"].strip()

        epoch = _extract_epoch_from_rg_match(
            data, [], filepath=match_filepath, epoch_cache=epoch_cache,
        )

        results.append(SearchResult(
            file_name=file_name,
            content=matched_text,
            epoch=epoch,
            score=_score_rg_match(data, query),
            source="rg",
            snippet=matched_text[:200],
        ))

    unique = {}
    for candidate in results:
        key = (candidate.file_name, candidate.epoch)
        if key not in unique or candidate.score > unique[key].score:
            unique[key] = candidate
    return list(unique.values())


def _score_rg_match(data: dict, query: str) -> float:
    """Score a ripgrep match based on match quality signals.

    Scoring signals:
      1. Submatch count: more submatches = better (diminishing returns)
      2. Match density: matched chars / total line length
      3. Exact query match: bonus if the full query appears in the line

    Returns a score in the range [0.1, 1.0].
    """
    line_text = data.get("lines", {}).get("text", "")
    submatches = data.get("submatches", [])

    # 1. Submatch count (diminishing returns via log-like scaling)
    n_submatches = max(len(submatches), 1)
    submatch_score = min(n_submatches / 3.0, 1.0)  # 3+ submatches → max

    # 2. Match density: total matched chars / line length
    matched_chars = sum(
        sm.get("end", 0) - sm.get("start", 0) for sm in submatches
    )
    line_len = max(len(line_text.strip()), 1)
    density_score = min(matched_chars / line_len, 1.0)

    # 3. Exact query match boost
    exact_boost = 0.2 if query.lower() in line_text.lower() else 0.0

    # 4. Term-coverage bonus: reward matching more distinct query terms (max 0.15)
    terms = query.lower().split()
    if len(terms) > 1:
        matched_terms = sum(1 for t in terms if t in line_text.lower())
        coverage_boost = 0.15 * (matched_terms / len(terms))
    else:
        coverage_boost = 0.0

    # Combine: weighted average + exact boost + coverage
    raw = 0.5 * submatch_score + 0.3 * density_score + exact_boost + coverage_boost
    return max(0.1, min(raw, 1.0))


def _extract_epoch_from_rg_match(
    data: dict, context: list[dict], filepath: Optional[Path] = None, epoch_cache=None,
) -> Optional[int]:
    """Extract the entry epoch from a ripgrep match and its before-context.

    Scans the context lines (displayed before the match) in reverse order
    looking for the nearest '>>> EPOCH' marker. If context scan fails and
    filepath is provided, falls back to scanning the file for the nearest
    preceding '>>>' marker relative to the match line number.
    """
    # The request-local map also handles short epochs and entry closing markers.
    if epoch_cache is not None and filepath is not None and data.get("line_number") is not None:
        return _scan_file_for_epoch(filepath, data["line_number"], epoch_cache)

    # Check the match line itself
    line_text = data["lines"]["text"]
    m = re.search(r">>> (\d{10,})", line_text)
    if m:
        return int(m.group(1))

    # Scan before-context lines in reverse (nearest first)
    for ctx in reversed(context):
        ctx_text = ctx.get("lines", {}).get("text", "")
        m = re.search(r">>> (\d{10,})", ctx_text)
        if m:
            return int(m.group(1))

    # Fallback: scan the actual file for the nearest preceding >>> marker
    if filepath is not None:
        match_line_no = data.get("line_number")
        if match_line_no is not None:
            return _scan_file_for_epoch(filepath, match_line_no, epoch_cache)

    return None


def _scan_file_for_epoch(filepath: Path, match_line_no: int, cache=None) -> Optional[int]:
    """Build a line-to-entry map at most once per file in a search request."""
    if cache is None:
        cache = {}
    if filepath not in cache:
        try:
            lines = filepath.read_text(encoding="utf-8").splitlines()
        except OSError:
            cache[filepath] = []
        else:
            epochs = []
            epoch = None
            for line in lines:
                marker = re.fullmatch(r">>> (\d+)\s*", line)
                if marker:
                    epoch = int(marker.group(1))
                elif re.fullmatch(r"<<<\s*", line):
                    epoch = None
                epochs.append(epoch)
            cache[filepath] = epochs
    epochs = cache[filepath]
    return epochs[match_line_no - 1] if 0 < match_line_no <= len(epochs) else None


def ripgrep_search_filenames(
    query: str,
    root: Path,
    timeout: float = 5.0,
) -> list[str]:
    """Search for files whose names match the query pattern."""
    if not RG_AVAILABLE:
        # Fallback to glob
        return [f.stem for f in root.glob("*.md") if query.lower() in f.stem.lower()]

    cmd = ["rg", "--files", "--glob", f"*{query}*.md", "--", str(root)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return [Path(line.strip()).stem for line in result.stdout.strip().split("\n") if line.strip()]
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []


class HybridSearch:
    """Orchestrates multi-method search across the knowledge base."""

    def __init__(self, config: KBConfig, db: KBDatabase):
        self.config = config
        self.db = db
        self.root = config.storage_dir

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
        exclude_archives: bool = True,
        author: Optional[str] = None,
        hostname: Optional[str] = None,
    ) -> list[SearchResult]:
        """Execute a search across configured methods.

        Args:
            query: Search terms. Ripgrep matches any literal whitespace-separated term;
                BM25 applies tokenization and stemming.
            mode: "hybrid" (rg + bm25), "rg" (ripgrep only),
                  "bm25" (FTS5 only).
            limit: Max results to return.
            domain: Optional domain prefix to scope search (e.g. "security").
            after_epoch: Only return entries after this epoch.
            before_epoch: Only return entries before this epoch.
            offset: Number of results to skip (for pagination).
            status: Filter by entry status.
            entry_type: Filter by entry type.
            exclude_archives: If True (default), exclude .archive files from results.

        Returns:
            Merged, deduplicated, ranked list of SearchResult.
        """
        results: list[SearchResult] = []
        # Fetch extra results to account for offset (sources don't know about offset)
        fetch_limit = limit + offset

        search_root = self.root
        if domain:
            # Ripgrep can be scoped by glob pattern
            pass  # Handled in rg call

        if mode in ("hybrid", "rg"):
            rg_query = query
            if domain:
                # Use rg's glob to scope to domain
                rg_results = self._rg_scoped(rg_query, domain, fetch_limit)
            else:
                rg_results = ripgrep_search(
                    rg_query, search_root, fetch_limit,
                    timeout=self.config.rg_timeout,
                )
            # Post-filter ripgrep results by epoch
            if after_epoch is not None or before_epoch is not None:
                rg_results = [
                    r for r in rg_results
                    if r.epoch is not None
                    and (after_epoch is None or r.epoch > after_epoch)
                    and (before_epoch is None or r.epoch < before_epoch)
                ]
            # Post-filter ripgrep results for archive exclusion
            if exclude_archives:
                rg_results = [r for r in rg_results if not r.file_name.endswith(".archive")]
            self._enrich_metadata(rg_results)
            rg_results = [r for r in rg_results
                          if (status is None or r.status == status)
                          and (entry_type is None or r.entry_type == entry_type)
                          and (author is None or r.author == author)
                          and (hostname is None or r.hostname == hostname)]
            results.extend(rg_results)

        if mode in ("hybrid", "bm25"):
            bm25_results = self.db.bm25_search_entries(
                query, fetch_limit,
                after_epoch=after_epoch, before_epoch=before_epoch,
                status=status, entry_type=entry_type,
                exclude_archives=exclude_archives,
                author=author, hostname=hostname, domain=domain,
            )
            results.extend(bm25_results)

        # Deduplicate, merge scores, enrich, boost, then apply offset
        merged = self._merge_results(results, len(results))
        self._enrich_metadata(merged)
        self._apply_boosts(merged)
        merged.sort(key=lambda x: x.score, reverse=True)
        return merged[offset:offset + limit]

    def search_files(
        self,
        query: str,
        limit: int = 5,
    ) -> list[dict]:
        """Search file metadata to find which files match a query.

        Used for routing content to the right file.
        """
        results = []

        # BM25 on file metadata
        bm25_files = self.db.bm25_search_files(query, limit)
        for f in bm25_files:
            f["_source"] = "bm25"
            f["_score"] = abs(f.get("rank", 0))
            results.append(f)

        # Filename text match
        name_matches = ripgrep_search_filenames(
            query, self.root, timeout=self.config.rg_timeout,
        )
        for name in name_matches:
            file_data = self.db.get_file(name)
            if file_data:
                file_data["_source"] = "filename"
                file_data["_score"] = 0.8
                results.append(file_data)

        # Deduplicate by name, keep highest score
        seen = {}
        for r in results:
            name = r.get("name", "")
            score = r.get("_score", 0)
            if name not in seen or score > seen[name].get("_score", 0):
                seen[name] = r

        deduped = sorted(seen.values(), key=lambda x: x.get("_score", 0), reverse=True)
        return deduped[:limit]

    def _rg_scoped(self, query: str, domain: str, limit: int) -> list[SearchResult]:
        """Ripgrep search scoped to a domain prefix."""
        results = ripgrep_search(query, self.root, limit, timeout=self.config.rg_timeout, domain=domain)
        return [r for r in results if r.file_name == domain or r.file_name.startswith(domain + ".")]

    def _enrich_metadata(self, results: list[SearchResult]) -> None:
        """Fill in metadata for results that lack it (e.g. rg-only results).

        Results from BM25 already carry status/entry_type/tags from the DB.
        Results from ripgrep don't, so we do a targeted DB lookup for each.
        """
        keys = [(r.file_name, r.epoch) for r in results if r.epoch is not None]
        rows = {(row["file_name"], row["epoch"]): row for row in self.db.get_entries_by_keys(keys)}
        for r in results:
            row = rows.get((r.file_name, r.epoch))
            if row:
                r.content = row["content"]
                r.status = row.get("status") or ""
                r.entry_type = row.get("entry_type") or ""
                r.tags = row.get("tags") or ""
                r.weight = row.get("weight") or "normal"
                r.author = row.get("author") or ""
                r.hostname = row.get("hostname") or ""

    # Boost constants
    RECENCY_HALF_LIFE_DAYS = 180  # 6 months
    TYPE_BOOST = {"decision": 1.1, "skill": 1.08, "finding": 1.05, "milestone": 1.05}
    STATUS_DAMPEN = {"superseded": 0.85, "resolved": 0.9, "archived": 0.7, "completed": 0.88, "cancelled": 0.65}
    STATUS_BOOST = {"pending": 1.08, "in_progress": 1.05}
    WEIGHT_BOOST = {"high": 1.15, "normal": 1.0, "low": 0.8}

    def _apply_boosts(self, results: list[SearchResult]) -> None:
        """Apply recency, type/status, weight, and staleness scoring boosts."""
        now = time.time()
        half_life = getattr(self.config, "recency_half_life_days", self.RECENCY_HALF_LIFE_DAYS)
        staleness_threshold = 2 * half_life
        for r in results:
            # Recency boost: 80% relevance + 20% recency-weighted relevance
            if r.epoch:
                age_days = (now - r.epoch) / 86400
                recency_factor = math.exp(-0.693 * age_days / half_life)
                r.score = 0.8 * r.score + 0.2 * r.score * recency_factor

                # Staleness penalty for old active entries (exempt decisions and high-weight)
                if age_days > staleness_threshold:
                    is_exempt = r.entry_type == "decision" or r.weight == "high"
                    if not is_exempt and r.status in ("active", ""):
                        staleness_ratio = min(age_days / staleness_threshold, 3.0)
                        penalty = max(0.7, 1.0 - 0.1 * (staleness_ratio - 1.0))
                        r.score *= penalty

            # Type boost
            r.score *= self.TYPE_BOOST.get(r.entry_type, 1.0)

            # Status dampen/boost
            if r.status in self.STATUS_DAMPEN:
                r.score *= self.STATUS_DAMPEN[r.status]
            elif r.status in self.STATUS_BOOST:
                r.score *= self.STATUS_BOOST[r.status]

            # Weight boost
            r.score *= self.WEIGHT_BOOST.get(r.weight, 1.0)

    def _merge_results(self, results: list[SearchResult], limit: int) -> list[SearchResult]:
        """Deduplicate and weighted-merge results from multiple sources."""
        # Key: (file_name, epoch or content hash)
        buckets: dict[str, SearchResult] = {}

        weights = {
            "rg": self.config.rg_weight,
            "bm25": self.config.bm25_weight,
        }

        for r in results:
            key = f"{r.file_name}:{r.epoch or hash(r.content[:100])}"
            if key in buckets:
                existing = buckets[key]
                # Accumulate weighted score
                existing.score += r.score * weights.get(r.source, 0.3)
                existing.source += f"+{r.source}"
            else:
                r.score = r.score * weights.get(r.source, 0.3)
                buckets[key] = r

        ranked = sorted(buckets.values(), key=lambda x: x.score, reverse=True)
        return ranked[:limit]
