"""Regression sequences using real Git repositories and Moto's S3 service."""
import hashlib
import json
import threading
import time
from unittest.mock import patch

import pytest

boto3 = pytest.importorskip("boto3")
pytest.importorskip("moto")
from moto import mock_aws

from hyperkb.config import KBConfig
from hyperkb.conflict import merge_versions
from hyperkb.format import parse_text
from hyperkb.locking import storage_lock
from hyperkb.remote import S3Remote, LOCK_TTL_SECONDS
from hyperkb.protocol import ProtocolError, ProtocolUnavailable
from hyperkb.sync import SyncEngine, SyncWorker


def document(*entries):
    return ("---\nname: shared.notes\ndescription: Notes\n---\n" + "".join(
        f"\n>>> {epoch}\n{content}\n<<<\n" for epoch, content in entries)).encode()


@pytest.fixture
def machines(tmp_path):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="sync-reliability")
        engines = []
        for name in ("a", "b"):
            config = KBConfig(root=str(tmp_path / name), sync_enabled=True,
                              sync_bucket="sync-reliability", sync_region="us-east-1")
            config.storage_dir.mkdir(parents=True)
            remote = S3Remote(config.sync_bucket, region=config.sync_region)
            engine = SyncEngine(config.storage_dir, remote, config, reindex_fn=lambda: None)
            engine.setup()
            engines.append(engine)
        yield (*engines, client)


def write(engine, content, name="shared.notes.md"):
    (engine.storage_dir / name).write_bytes(content)


def read(engine, name="shared.notes.md"):
    return (engine.storage_dir / name).read_bytes()


def establish(a, b):
    write(a, document((100, "base")))
    a.sync()
    b.sync()


def remote_content(engine, name="shared.notes.md"):
    entry = engine.remote.get_manifest()["files"][name]
    return engine.remote.download_version(entry["blob"]) if entry.get("blob") else engine.remote.download_file(name)


def test_pull_never_acknowledges_pending_upload(machines):
    a, b, _ = machines
    establish(a, b)
    write(a, document((100, "base"), (200, "acknowledged local write")))
    a.sync("pull")
    assert "shared.notes.md" in a.get_status()["local_pending_files"]
    a.sync("both")
    b.sync()
    assert b"acknowledged local write" in read(b)


def test_push_preserves_unseen_remote_inventory(machines):
    a, b, _ = machines
    establish(a, b)
    write(b, document((200, "only b")), "remote.notes.md")
    b.sync()
    write(a, document((300, "only a")), "local.notes.md")
    a.sync("push")
    assert set(a.remote.get_manifest()["files"]) == {
        "shared.notes.md", "remote.notes.md", "local.notes.md"}
    a.sync()
    assert b"only b" in read(a, "remote.notes.md")


def test_independent_first_sync_additions_survive(machines):
    a, b, _ = machines
    write(a, document((100, "A")), "a.notes.md")
    write(b, document((200, "B")), "b.notes.md")
    a.sync()
    b.sync()
    a.sync()
    assert read(a, "b.notes.md") == read(b, "b.notes.md")
    assert read(a, "a.notes.md") == read(b, "a.notes.md")


def test_concurrent_appends_merge_then_upload(machines):
    a, b, _ = machines
    establish(a, b)
    write(a, document((100, "base"), (200, "A")))
    write(b, document((100, "base"), (300, "B")))
    a.sync()
    b.sync()
    a.sync()
    assert read(a) == read(b) == remote_content(a)
    assert {e.content for e in parse_text(read(a).decode())[1]} == {"base", "A", "B"}


def test_push_conflict_fails_without_overwriting(machines):
    a, b, _ = machines
    establish(a, b)
    write(a, document((100, "A")))
    write(b, document((100, "B")))
    b.sync()
    with pytest.raises(RuntimeError, match="Concurrent remote"):
        a.sync("push")
    assert b"B" in remote_content(a)
    assert b"A" in read(a)
    a.sync()
    assert {e.content for e in parse_text(read(a).decode())[1]} == {"A", "B"}


def test_last_remote_file_deletion_reindexes(machines):
    a, b, _ = machines
    establish(a, b)
    (a.storage_dir / "shared.notes.md").unlink()
    a.sync()
    indexed = []
    b.reindex_fn = lambda: indexed.append(list(b.storage_dir.glob("*.md")))
    result = b.sync()
    assert result["deleted"] == ["shared.notes.md"]
    assert indexed == [[]]
    assert not (b.storage_dir / "shared.notes.md").exists()


@pytest.mark.parametrize("delete_side", ["local", "remote"])
def test_modify_delete_preserves_modification(machines, delete_side):
    a, b, _ = machines
    establish(a, b)
    writer, deleter = (a, b) if delete_side == "remote" else (b, a)
    write(writer, document((100, "modified")))
    (deleter.storage_dir / "shared.notes.md").unlink()
    b.sync()
    a.sync()
    b.sync()
    assert read(a) == read(b) == document((100, "modified"))


def test_manifest_failure_keeps_previous_remote_snapshot(machines):
    a, b, _ = machines
    establish(a, b)
    original = remote_content(a)
    write(a, document((100, "new revision")))
    with patch.object(a.remote, "put_manifest", side_effect=OSError("interrupted")):
        with pytest.raises(OSError, match="interrupted"):
            a.sync()
    assert remote_content(a) == original
    a.sync()
    b.sync()
    assert b"new revision" in read(b)


@pytest.mark.parametrize("name, content, digest", [
    ("../escape.md", b"unsafe", None),
    ("shared.notes.md", b"corrupted", "0" * 64),
])
def test_invalid_download_does_not_change_live_storage(machines, name, content, digest):
    a, b, _ = machines
    establish(a, b)
    original = read(b)
    a.remote.upload_file(name, content)
    a.remote.put_manifest({"version": 2, "files": {name: {"sha256": digest or hashlib.sha256(content).hexdigest()}}})
    with pytest.raises(ValueError):
        b.sync()
    assert read(b) == original
    assert b.git.get_current_branch() in ("main", "master")
    assert a.remote.check_lock() is None


def test_reindex_failure_is_retried_on_next_sync(machines):
    a, b, _ = machines
    establish(a, b)
    write(a, document((100, "updated")))
    a.sync()
    b.reindex_fn = lambda: (_ for _ in ()).throw(OSError("index failure"))
    with pytest.raises(OSError, match="index failure"):
        b.sync()
    calls = []
    b.reindex_fn = lambda: calls.append(1)
    b.sync()
    assert calls == [1]


def test_acknowledged_writer_cannot_interleave_publication(machines):
    a, b, _ = machines
    establish(a, b)
    write(a, document((100, "remote updated")))
    a.sync()
    downloading, writer_done = threading.Event(), threading.Event()
    download = b.remote.download_version
    def pause(digest):
        downloading.set()
        assert not writer_done.wait(0.1)
        return download(digest)
    def writer():
        assert downloading.wait(3)
        with storage_lock(b.storage_dir):
            write(b, read(b) + b"\n>>> 200\nacknowledged\n<<<\n")
            b.git.commit_all_pending()
        writer_done.set()
    thread = threading.Thread(target=writer)
    thread.start()
    with patch.object(b.remote, "download_version", side_effect=pause):
        b.sync()
    thread.join(3)
    assert writer_done.is_set()
    assert b"remote updated" in read(b) and b"acknowledged" in read(b)
    b.sync()
    assert b"acknowledged" in remote_content(b)


def test_disabled_config_blocks_network(machines):
    a, _, _ = machines
    a.config.sync_enabled = False
    a.config.save()
    with patch.object(a.remote, "acquire_lock") as acquire:
        assert a.sync()["status"] == "disabled"
        acquire.assert_not_called()


def test_changed_target_uses_new_remote_and_baseline(machines):
    a, _, client = machines
    write(a, document((100, "local")))
    a.sync()
    client.create_bucket(Bucket="sync-new-target")
    a.config.sync_bucket = "sync-new-target"
    a.config.save()
    a.sync()
    assert a.remote.bucket == "sync-new-target"
    assert b"local" in remote_content(a)


def test_sync_reads_manifest_only_after_acquiring(machines):
    a, _, _ = machines
    acquire = a.remote.acquire_lock
    order = []
    def acquire_record():
        order.append("acquire")
        return acquire()
    get = a.remote.get_manifest
    def get_record():
        order.append("manifest")
        return get()
    with patch.object(a.remote, "acquire_lock", side_effect=acquire_record), \
         patch.object(a.remote, "get_manifest", side_effect=get_record):
        a.sync()
    assert order == ["acquire", "manifest"]


def test_remote_lease_concurrent_acquisition_and_stale_release(machines):
    a, b, client = machines
    barrier = threading.Barrier(2)
    outcomes = []
    def compete(remote):
        read_lock = remote._read_lock
        def race():
            value = read_lock()
            barrier.wait(3)
            return value
        with patch.object(remote, "_read_lock", side_effect=race):
            outcomes.append((remote, remote.acquire_lock()))
    threads = [threading.Thread(target=compete, args=(e.remote,)) for e in (a, b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    assert sorted(result for _, result in outcomes) == [False, True]
    winner = next(remote for remote, result in outcomes if result)
    loser = next(remote for remote, result in outcomes if not result)
    assert winner.renew_lock()
    client.put_object(Bucket=winner.bucket, Key="hkb/_sync/lock.json", Body=json.dumps({
        "timestamp": time.time() - LOCK_TTL_SECONDS - 1, "lease_id": winner._lease_id}).encode())
    assert loser.acquire_lock()
    winner.release_lock()
    assert loser.check_lock()["lease_id"] == loser._lease_id
    assert loser.renew_lock()


def test_entry_three_way_deletion_and_metadata():
    base = document((100, "delete me"), (200, "@author: alice\nold"))
    ours = document((200, "@author: alice\nold"), (300, "local"))
    theirs = document((100, "delete me"), (200, "@author: bob\nnew"))
    merged, _ = merge_versions(base, ours, theirs, "shared.notes.md")
    entries = parse_text(merged.decode())[1]
    assert [e.epoch for e in entries] == [200, 300]
    assert entries[0].metadata["author"] == "bob"


def test_workers_elect_one_leader_and_take_over(machines):
    a, _, _ = machines
    other = SyncEngine(a.storage_dir, a.remote, a.config)
    calls = [threading.Event(), threading.Event()]
    with patch("hyperkb.sync.WATCHDOG_AVAILABLE", False), \
         patch.object(a, "sync", side_effect=lambda: calls[0].set()), \
         patch.object(other, "sync", side_effect=lambda: calls[1].set()):
        first = SyncWorker(a, interval=60)
        second = SyncWorker(other, interval=60)
        first.start()
        try:
            assert calls[0].wait(3)
            second.start()
            assert not calls[1].wait(0.2)
            assert first.is_leader and not second.is_leader
            first.stop()
            first.join(2)
            assert calls[1].wait(3)
            assert second.is_leader
        finally:
            first.stop()
            second.stop()
            first.join(2)
            if second.ident:
                second.join(2)
        assert not first.is_alive() and not second.is_alive()


def test_worker_request_wakes_without_clearing_stop(machines):
    a, _, _ = machines
    calls = []
    fired = threading.Event()
    def sync():
        calls.append(1)
        fired.set()
    with patch("hyperkb.sync.WATCHDOG_AVAILABLE", False), patch.object(a, "sync", side_effect=sync):
        worker = SyncWorker(a, interval=60)
        worker.start()
        try:
            assert fired.wait(3)
            fired.clear()
            worker.request_sync()
            assert fired.wait(1)
            worker.stop()
            worker.request_sync()
            assert worker._stop_event.is_set()
        finally:
            worker.stop()
            worker.join(2)
        assert len(calls) >= 2
        assert not worker.is_alive()


def test_worker_stop_cancels_storage_lock_wait(machines):
    a, _, _ = machines
    with patch("hyperkb.sync.WATCHDOG_AVAILABLE", False), storage_lock(a.storage_dir):
        worker = SyncWorker(a, interval=60)
        worker.start()
        time.sleep(0.1)
        worker.stop()
        worker.join(1)
        assert not worker.is_alive()


def test_leader_process_exit_allows_automatic_takeover(tmp_path):
    import os
    import subprocess
    import sys
    script = '''
import sys, time
from pathlib import Path
from types import SimpleNamespace
from hyperkb.sync import SyncWorker
import hyperkb.sync
hyperkb.sync.WATCHDOG_AVAILABLE = False
import threading
storage, marker = map(Path, sys.argv[1:])
engine = SimpleNamespace(storage_dir=storage, config=SimpleNamespace(sync_interval=60),
                         _cancel_event=threading.Event(), sync=lambda: marker.write_text("leader"))
worker = SyncWorker(engine, interval=60)
worker.start()
while True:
    time.sleep(1)
'''
    storage = tmp_path / "storage"
    storage.mkdir()
    first_marker, second_marker = tmp_path / "first", tmp_path / "second"
    def wait_for(path):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if path.exists():
                return True
            time.sleep(0.025)
        return False
    first = subprocess.Popen([sys.executable, "-c", script, str(storage), str(first_marker)])
    second = None
    try:
        assert wait_for(first_marker)
        second = subprocess.Popen([sys.executable, "-c", script, str(storage), str(second_marker)])
        time.sleep(0.25)
        assert not second_marker.exists()
        first.terminate()
        first.wait(3)
        assert wait_for(second_marker)
    finally:
        for process in (first, second):
            if process and process.poll() is None:
                process.terminate()
                process.wait(3)


def test_protocol_version_and_rebuild_preserve_tombstones(machines):
    a, b, client = machines
    establish(a, b)
    (a.storage_dir / "shared.notes.md").unlink()
    a.sync()
    manifest = a.remote.get_manifest()
    assert manifest["version"] == 2
    assert "shared.notes.md" in manifest["tombstones"]
    assert a.remote.rebuild_manifest() == manifest
    assert a.remote.get_manifest() == manifest
    client.delete_object(Bucket=a.remote.bucket, Key="hkb/_sync/manifest.json")
    with pytest.raises(ValueError, match="restore"):
        a.remote.rebuild_manifest()
    with pytest.raises(ProtocolError, match="manifest is missing"):
        a.remote.get_manifest()


def test_tombstone_prevents_unmodified_first_sync_resurrection(machines):
    a, b, _ = machines
    write(a, document((100, "deleted")))
    write(b, document((100, "deleted")))
    a.sync()
    (a.storage_dir / "shared.notes.md").unlink()
    a.sync()
    result = b.sync()
    assert result["deleted"] == ["shared.notes.md"]
    assert not (b.storage_dir / "shared.notes.md").exists()
    assert not a.remote.get_manifest()["files"]


def test_unknown_manifest_version_rejected_without_changes(machines):
    a, b, client = machines
    establish(a, b)
    old = read(b)
    client.put_object(Bucket=a.remote.bucket, Key="hkb/_sync/manifest.json",
                      Body=json.dumps({"version": 999, "files": {}}).encode())
    with pytest.raises(ProtocolError):
        b.sync()
    assert read(b) == old
    assert a.remote.check_lock() is None


def test_concurrent_merge_never_drops_unparsed_text():
    base = document((100, "base"))
    ours = document((100, "local")) + b"Important unstructured notes\n"
    theirs = document((100, "remote"))
    with pytest.raises(ValueError, match="outside entries"):
        merge_versions(base, ours, theirs, "shared.notes.md")


def test_interrupted_first_upload_can_retry(machines):
    a, b, _ = machines
    write(a, document((100, "first")))
    upload = a.remote.upload_version
    def fail_after_upload(content):
        upload(content)
        raise OSError("interrupted first upload")
    with patch.object(a.remote, "upload_version", side_effect=fail_after_upload):
        with pytest.raises(OSError, match="interrupted first"):
            a.sync()
    assert a.remote.get_manifest()["files"] == {}
    a.sync()
    b.sync()
    assert read(b) == read(a)


@pytest.mark.parametrize("version", [True, False, 1.0, 2.0, "2", None])
def test_manifest_version_requires_exact_integer(machines, version):
    a, b, client = machines
    write(b, document((100, "keep local")))
    client.put_object(Bucket=a.remote.bucket, Key="hkb/_sync/manifest.json",
                      Body=json.dumps({"version": version, "files": {}}).encode())
    with pytest.raises(ProtocolError):
        b.sync()
    assert b"keep local" in read(b)
    with pytest.raises(ValueError, match="Unsupported"):
        a.remote.put_manifest({"version": version, "files": {}})


def test_missing_legacy_manifest_requires_explicit_rebuild(machines):
    a, _, _ = machines
    remote_bytes = document((100, "unseen legacy remote"))
    a.remote.upload_file("legacy.notes.md", remote_bytes)
    write(a, document((200, "local addition")))
    with pytest.raises(ProtocolError, match="rebuild legacy inventory"):
        a.sync()
    assert a.remote.download_file("legacy.notes.md") == remote_bytes
    rebuilt = a.remote.rebuild_manifest()
    assert set(rebuilt["files"]) == {"legacy.notes.md"}
    a.remote.upgrade_protocol()
    a.sync()
    assert set(a.remote.get_manifest()["files"]) == {"legacy.notes.md", "shared.notes.md"}
    assert read(a, "legacy.notes.md") == remote_bytes


def test_delayed_publisher_cannot_overwrite_successor_inventory(machines):
    a, b, client = machines
    establish(a, b)
    write(a, document((100, "A pending")))
    write(b, document((100, "B successor")))
    publish = a.remote.put_manifest
    def delayed_publish(manifest, **kwargs):
        # A passed its lease guard, then paused until its lease expired. B
        # acquires a fresh lease and publishes before A's HTTP write arrives.
        client.put_object(Bucket=a.remote.bucket, Key="hkb/_sync/lock.json",
                          Body=json.dumps({"timestamp": time.time() - LOCK_TTL_SECONDS - 1,
                                           "lease_id": a.remote._lease_id}).encode())
        b.sync()
        return publish(manifest, **kwargs)
    with patch.object(a.remote, "put_manifest", side_effect=delayed_publish):
        with pytest.raises(RuntimeError, match="manifest changed"):
            a.sync()
    assert remote_content(b) == read(b) == document((100, "B successor"))
    assert read(a) == document((100, "A pending"))
    a.sync()
    assert {entry.content for entry in parse_text(read(a).decode())[1]} == {"A pending", "B successor"}


def test_conditional_manifest_revisions_prevent_aba(machines):
    a, b, _ = machines
    initial = {"version": 2, "files": {}}
    a.remote.put_manifest(initial, expected_etag=None)
    a.remote.get_manifest()
    stale_etag = a.remote.manifest_etag
    b.remote.get_manifest()
    b.remote.put_manifest(initial, expected_etag=b.remote.manifest_etag)
    with pytest.raises(RuntimeError, match="manifest changed"):
        a.remote.put_manifest(initial, expected_etag=stale_etag)


def test_atomic_sync_write_skips_directory_fsync_on_windows(tmp_path):
    import os
    path = tmp_path / "snapshot.json"
    with patch("hyperkb.sync.os", wraps=os) as sync_os:
        sync_os.name = "nt"
        sync_os.open.side_effect = AssertionError("Windows cannot open a directory for fsync")
        SyncEngine._atomic_write(path, b"complete snapshot")
        sync_os.open.assert_not_called()
    assert path.read_bytes() == b"complete snapshot"


def test_sync_protocol_mismatch_precedes_local_git_and_index_mutations(machines):
    a, _, client = machines
    before = a.git.get_head_sha()
    write(a, document((100, "pending local edit")))
    dirty = a.storage_dir.parent / "sync" / "reindex-needed"
    dirty.parent.mkdir(exist_ok=True)
    dirty.write_text("pending")
    client.put_object(Bucket=a.remote.bucket, Key="hkb/_sync/protocol.json",
                      Body=json.dumps({"protocol_version": 3}).encode())
    with patch.object(a, "reindex_fn") as reindex, pytest.raises(ProtocolError):
        a.sync()
    assert a.git.get_head_sha() == before
    assert dirty.exists()
    assert b"pending local edit" in read(a)
    reindex.assert_not_called()


def test_sync_dry_run_does_not_mutate_git_index_or_remote(machines):
    a, _, _ = machines
    before = a.git.get_head_sha()
    write(a, document((100, "uncommitted")))
    with patch.object(a.remote._client, "put_object") as put, \
         patch.object(a.remote._client, "delete_object") as delete, \
         patch.object(a, "reindex_fn") as reindex:
        assert a.sync(dry_run=True)["status"] == "dry_run"
    assert a.git.get_head_sha() == before
    assert a.git.has_uncommitted_changes()
    put.assert_not_called()
    delete.assert_not_called()
    reindex.assert_not_called()


def test_legacy_offline_edits_reconcile_after_explicit_upgrade(machines):
    from hyperkb.store import KnowledgeStore
    a, _, _ = machines
    remote_bytes = document((100, "remote before upgrade"))
    a.remote.upload_file("shared.notes.md", remote_bytes)
    a.remote.put_manifest({"files": {"shared.notes.md": {
        "sha256": hashlib.sha256(remote_bytes).hexdigest(), "size": len(remote_bytes)}}})
    store = KnowledgeStore(a.config)
    store.init()
    try:
        store.create_file(name="shared.notes", description="Notes", keywords=["notes"])
        store.add_entry(file_name="shared.notes", content="local recorded while disconnected", epoch=200)
        with pytest.raises(ProtocolError) as error:
            a.sync()
        assert error.value.status == "store_upgrade_required"
        # A successful store write remains possible while remote sync is fenced.
        store.add_entry(file_name="shared.notes", content="more local recording", epoch=300)
        local_before = read(a)
        plan = a.remote.upgrade_protocol(dry_run=True)
        assert plan["transformation"]["markdown_changed"] is False
        a.remote.upgrade_protocol()
        assert read(a) == local_before
        a.reindex_fn = store.reindex
        a.sync()
        contents = {entry.content for entry in parse_text(read(a).decode())[1]}
        assert contents == {"remote before upgrade", "local recorded while disconnected", "more local recording"}
        assert remote_content(a) == read(a)
        assert store.db.get_entry("shared.notes", 100)["content"] == "remote before upgrade"
        assert store.db.get_entry("shared.notes", 300)["content"] == "more local recording"
    finally:
        store.close()
