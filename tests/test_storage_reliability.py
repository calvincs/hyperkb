"""Regression coverage for publication, shared ownership and index recovery."""
import json
import multiprocessing
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from hyperkb.config import KBConfig
from hyperkb.format import parse_file
from hyperkb.locking import FileLock, storage_lock
from hyperkb.store import KnowledgeStore


def _write_process(root, barrier):
    store = KnowledgeStore(KBConfig(root=root))
    store.recover_index()
    barrier.wait(timeout=15)
    for i in range(8):
        store.add_entry(f"process note {i}", "test.shared", epoch=1000)
    store.close()


def test_processes_preserve_colliding_acknowledged_writes(kb_store):
    kb_store.create_file("test.shared", "Shared notes", [])
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    processes = [context.Process(target=_write_process, args=(str(kb_store.root), barrier)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    _, entries = parse_file(kb_store.storage_dir / "test.shared.md")
    assert len(entries) == len({entry.epoch for entry in entries}) == 16
    assert len(kb_store.db.get_entries("test.shared")) == 16


def test_storage_lock_shared_reentrant_and_nonblocking(tmp_path):
    first = storage_lock(tmp_path)
    assert first is storage_lock(tmp_path / ".")
    with first, storage_lock(tmp_path):
        other = FileLock(first.path, timeout=0)
        assert not other.try_acquire()
    assert other.try_acquire()
    other.release()


@pytest.mark.parametrize("replacement", ["replacement\n<<<\nlost", "replacement\n>>> 1234\ninjected", "x" * 200])
def test_update_uses_creation_content_validation(kb_store, replacement):
    kb_store.create_file("test.atomic", "Atomic notes", [])
    kb_store.add_entry("original", "test.atomic", epoch=1000)
    kb_store.config.max_entry_size = 100
    path = kb_store.storage_dir / "test.atomic.md"
    original = path.read_bytes()
    with pytest.raises(ValueError):
        kb_store.update_entry("test.atomic", 1000, new_content=replacement)
    assert path.read_bytes() == original
    assert kb_store.db.get_entry("test.atomic", 1000)["content"] == "original"


def test_failed_replace_preserves_source_and_index(kb_store, monkeypatch):
    import hyperkb.format as fmt
    kb_store.create_file("test.atomic", "Atomic notes", [])
    kb_store.add_entry("original", "test.atomic", epoch=1000)
    path = kb_store.storage_dir / "test.atomic.md"
    original = path.read_bytes()
    def fail(*args):
        raise OSError("injected before publication")
    monkeypatch.setattr(fmt.os, "replace", fail)
    with pytest.raises(OSError):
        kb_store.update_entry("test.atomic", 1000, new_content="replacement")
    assert path.read_bytes() == original
    assert kb_store.db.get_entry("test.atomic", 1000)["content"] == "original"
    assert not list(kb_store.storage_dir.glob("*.tmp"))


def test_archive_interruption_preserves_originals_and_retry_deduplicates(kb_store, monkeypatch):
    import hyperkb.store as module
    kb_store.create_file("test.atomic", "Atomic notes", [])
    kb_store.add_entry("original", "test.atomic", epoch=1000)
    original_write = module.atomic_write_text
    def fail_source(path, *args, **kwargs):
        if path.name == "test.atomic.md":
            raise OSError("interrupted after archive publication")
        return original_write(path, *args, **kwargs)
    monkeypatch.setattr(module, "atomic_write_text", fail_source)
    with pytest.raises(OSError):
        kb_store.archive_entry("test.atomic", 1000)
    assert len(parse_file(kb_store.storage_dir / "test.atomic.md")[1]) == 1
    assert len(parse_file(kb_store.storage_dir / "test.atomic.archive.md")[1]) == 1
    monkeypatch.setattr(module, "atomic_write_text", original_write)
    kb_store.archive_entry("test.atomic", 1000)
    assert len(kb_store.db.get_entries("test.atomic.archive")) == 1
    assert not kb_store.db.get_entries("test.atomic")


def test_archive_preserves_provenance_and_rebuilds_links(kb_store):
    kb_store.create_file("test.source", "Source notes", [])
    kb_store.create_file("test.target", "Target notes", [])
    kb_store.add_entry("@author: agent-a\n@hostname: host-a\n@weight: high\n@type: decision\nChoice [[test.target]]", "test.source", epoch=1000)
    kb_store.add_entry("Reference [[test.source#1000]]", "test.target", epoch=1001)
    kb_store.archive_entry("test.source", 1000)
    archived = kb_store.db.get_entry("test.source.archive", 1000)
    assert (archived["author"], archived["hostname"], archived["weight"], archived["entry_type"]) == ("agent-a", "host-a", "high", "decision")
    assert kb_store.db.get_entry_references("test.source", 1000) == []
    assert kb_store.db.get_entry_references("test.source.archive", 1000)[0]["target_file"] == "test.target"
    assert kb_store.db.get_entry_references("test.target", 1001)[0]["target_file"] == "test.source.archive"
    kb_store.reindex()
    assert kb_store.db.get_entry_references("test.target", 1001)[0]["target_file"] == "test.source.archive"


@pytest.mark.parametrize("damage", ["missing", "corrupt", "stale"])
def test_recover_index_restores_markdown_and_preserves_corrupt_evidence(kb_store, damage):
    kb_store.create_file("test.recover", "Recovery notes", [])
    kb_store.add_entry("durable entry", "test.recover", epoch=1000)
    kb_store.close()
    path = kb_store.config.db_path
    if damage == "missing":
        path.unlink()
    elif damage == "corrupt":
        path.write_bytes(b"corrupt evidence")
    else:
        kb_store.db.update_entry("test.recover", 1000, content="stale")
    assert "Reindexed" in kb_store.recover_index()
    assert kb_store.db.get_entry("test.recover", 1000)["content"] == "durable entry"
    if damage == "corrupt":
        copies = list(path.parent.glob(path.name + ".corrupt-*"))
        assert any(copy.read_bytes() == b"corrupt evidence" for copy in copies)


def test_reindex_snapshot_serializes_concurrent_writer(kb_store, monkeypatch):
    import hyperkb.store as module
    kb_store.create_file("test.race", "Race notes", [])
    kb_store.add_entry("first", "test.race", epoch=1000)
    parsed = threading.Event()
    release = threading.Event()
    original_parse = module.parse_file
    def blocked_parse(path):
        result = original_parse(path)
        parsed.set()
        assert release.wait(timeout=5)
        return result
    monkeypatch.setattr(module, "parse_file", blocked_parse)
    with ThreadPoolExecutor(max_workers=2) as pool:
        rebuild = pool.submit(kb_store.reindex)
        assert parsed.wait(timeout=5)
        write = pool.submit(kb_store.add_entry, "second", "test.race", 1001)
        assert not write.done()
        release.set()
        rebuild.result(timeout=5)
        write.result(timeout=5)
    assert len(kb_store.db.get_entries("test.race")) == 2


@pytest.mark.parametrize("name", ["../outside", "/tmp/outside", "test/escape", "test..escape", "test\\escape"])
def test_read_and_mutation_paths_reject_traversal(kb_store, name):
    with pytest.raises(ValueError):
        kb_store.show_file(name)
    with pytest.raises(ValueError):
        kb_store.archive_entry(name, 1000)


def test_compaction_preserves_tasks_and_decisions(kb_store):
    kb_store.create_file("test.semantic", "Semantic notes", [])
    for i, kind in enumerate(["task", "decision", "task"]):
        kb_store.add_entry(f"@type: {kind}\nImportant item {i}", "test.semantic", epoch=1000 + i)
    result = kb_store.compact_file("test.semantic", min_cluster=1, min_age_seconds=0, dry_run=False)
    assert result["entries_archived"] == 0
    assert len(kb_store.db.get_entries("test.semantic")) == 3


def test_reader_cannot_observe_same_connection_mid_rebuild(kb_store):
    kb_store.create_file("test.reader", "Reader notes", [])
    kb_store.add_entry("stable", "test.reader", epoch=1000)
    deleted = threading.Event()
    release = threading.Event()
    def trace(sql):
        if sql == "DELETE FROM files":
            deleted.set()
            assert release.wait(timeout=5)
    kb_store.db.connect().set_trace_callback(trace)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            rebuild = pool.submit(kb_store.reindex)
            assert deleted.wait(timeout=5)
            reader = pool.submit(kb_store.db.get_entries, "test.reader")
            assert not reader.done()
            release.set()
            rebuild.result(timeout=5)
            assert len(reader.result(timeout=5)) == 1
    finally:
        kb_store.db.connect().set_trace_callback(None)


def test_show_preserves_entry_metadata(kb_store):
    kb_store.create_file("test.metadata", "Metadata notes", [])
    kb_store.add_entry("@type: task\n@status: pending\n@author: client-a\nWork item", "test.metadata", epoch=1000)
    entry = kb_store.show_file("test.metadata")["entries"][0]
    assert entry["metadata"]["status"] == "pending"
    assert entry["metadata"]["type"] == "task"
    assert entry["metadata"]["author"] == "client-a"


def test_index_reads_do_not_wait_for_another_storage_mutation(kb_store):
    kb_store.create_file("test.readonly", "Read-only notes", [])
    kb_store.add_entry("committed snapshot", "test.readonly", epoch=1000)
    # Sync can hold the process lock while waiting for network; ordinary SQL
    # readers should continue to see the committed index during that interval.
    with kb_store._write_lock, ThreadPoolExecutor(max_workers=1) as pool:
        entries = pool.submit(kb_store.db.get_entries, "test.readonly").result(timeout=2)
    assert entries[0]["content"] == "committed snapshot"
