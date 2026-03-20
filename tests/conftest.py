"""Shared fixtures for hyperkb tests."""

import json
import pytest
from pathlib import Path

from hyperkb.config import KBConfig, HKB_DIR, CONFIG_FILENAME
from hyperkb.db import KBDatabase
from hyperkb.store import KnowledgeStore
from hyperkb.format import FileHeader, Entry, create_file_content, append_entry_to_file


@pytest.fixture
def kb_root(tmp_path):
    """A temporary directory for a knowledge base."""
    return tmp_path


@pytest.fixture
def kb_config(kb_root):
    """A KBConfig with embeddings disabled for fast tests."""
    return KBConfig(root=str(kb_root))


@pytest.fixture
def kb_db(kb_config):
    """An initialized KBDatabase."""
    # Ensure .hkb/ directory exists for SQLite
    Path(kb_config.root, ".hkb").mkdir(parents=True, exist_ok=True)
    db = KBDatabase(kb_config)
    db.connect()
    db.init_schema()
    yield db
    db.close()


@pytest.fixture
def kb_store(kb_config):
    """An initialized KnowledgeStore (embeddings disabled)."""
    store = KnowledgeStore(kb_config)
    store.init()
    yield store
    store.close()


@pytest.fixture
def sample_kb(kb_store):
    """A pre-populated KB with 2 files and 3 entries at known epochs.

    Files:
        security.threat-intel  (epochs: 1000000, 2000000)
        fitness.kettlebell     (epoch: 3000000)
    """
    kb_store.create_file(
        name="security.threat-intel",
        description="Threat intelligence findings and IOC observations.",
        keywords=["threat", "intel", "ioc", "feeds"],
    )
    kb_store.create_file(
        name="fitness.kettlebell",
        description="Kettlebell workout programming and progress notes.",
        keywords=["kettlebell", "workout", "press", "swing"],
    )

    kb_store.add_entry(
        content="AlienVault OTX has 6hr delay vs abuse.ch.",
        file_name="security.threat-intel",
        epoch=1000000,
    )
    kb_store.add_entry(
        content="MISP dedup rate is 40% across feeds. See [[fitness.kettlebell]]",
        file_name="security.threat-intel",
        epoch=2000000,
    )
    kb_store.add_entry(
        content="New PR: 5x5 double KB press at 24kg.",
        file_name="fitness.kettlebell",
        epoch=3000000,
    )

    return kb_store
