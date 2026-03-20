"""Data models for hyperkb."""

from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class FileHeader:
    """Parsed YAML frontmatter from a knowledge file."""
    name: str
    description: str
    keywords: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)
    created: str = ""
    compacted: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "keywords": self.keywords,
            "links": self.links,
            "created": self.created,
            "compacted": self.compacted,
        }


@dataclass
class Entry:
    """A single timestamped entry within a knowledge file."""
    epoch: int
    content: str
    file_name: str = ""
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.epoch)


@dataclass
class SearchResult:
    """A search result from any search method."""
    file_name: str
    content: str
    epoch: Optional[int] = None
    score: float = 0.0
    source: str = ""  # "rg", "bm25"
    snippet: str = ""
    status: str = ""
    entry_type: str = ""
    tags: str = ""
    weight: str = ""
    author: str = ""
    hostname: str = ""


@dataclass
class FileCandidate:
    """A candidate file for routing content to."""
    name: str
    description: str
    keywords: list[str]
    score: float = 0.0
    reason: str = ""
