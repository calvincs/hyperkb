"""Regression coverage for consistent filters, full context and request budgets."""
import json
from pathlib import Path

import pytest

from hyperkb.search import RG_AVAILABLE, ripgrep_search


@pytest.mark.parametrize("mode", ["rg", "hybrid", "bm25"])
def test_search_metadata_filters_apply_to_all_candidates(kb_store, mode):
    kb_store.create_file("test.filter", "Filtered notes", [])
    kb_store.add_entry("@status: completed\n@type: task\n@author: other\n@hostname: other\nmatchingword", "test.filter", epoch=1000000000)
    kb_store.add_entry("@status: active\n@type: decision\n@author: mine\n@hostname: here\nmatchingword", "test.filter", epoch=1000000001)
    results = kb_store.search("matchingword", mode=mode, domain="test", limit=1, status="active", entry_type="decision", author="mine", hostname="here")
    if mode == "rg" and not RG_AVAILABLE:
        pytest.skip("ripgrep unavailable")
    assert len(results) == 1
    assert results[0].epoch == 1000000001


def test_domain_filter_precedes_bm25_limit(kb_store):
    kb_store.create_file("other.notes", "Other notes", [])
    kb_store.create_file("target.notes", "Target notes", [])
    for epoch in range(1000, 1020):
        kb_store.add_entry("running", "other.notes", epoch=epoch)
    kb_store.add_entry("We are running a complicated operation with many other words", "target.notes", epoch=2000)
    results = kb_store.search("run", mode="bm25", domain="target", limit=1)
    assert len(results) == 1
    assert results[0].file_name == "target.notes"
    assert not kb_store.search("run", mode="bm25", domain="tar", limit=1)


def test_deep_context_retains_nonmatching_caveat(kb_store):
    kb_store.create_file("test.context", "Context notes", [])
    full = "matchingword summary\nCritical caveat: never skip validation.\nRecovery instructions: restore the original."
    kb_store.add_entry(full, "test.context", epoch=1000000000)
    result = kb_store.build_context("matchingword", max_tokens=2000)
    assert result["entries"][0]["content"] == full


def test_context_budget_counts_complete_serialized_response(kb_store):
    for i in range(12):
        name = f"test.file{i}"
        kb_store.create_file(name, "Description " * 30, [])
        kb_store.add_entry("matchingword", name, epoch=1000000000 + i)
    result = kb_store.build_context("matchingword", max_tokens=300)
    assert result["tokens_used"] == kb_store._estimate_tokens(json.dumps(result, indent=2))
    assert result["tokens_used"] <= 300


@pytest.mark.skipif(not RG_AVAILABLE, reason="ripgrep unavailable")
def test_rg_reads_each_matched_file_at_most_once(kb_store, monkeypatch):
    kb_store.create_file("test.epochs", "Epoch notes", [])
    kb_store.add_entry("\n".join("matchingword line" for _ in range(300)), "test.epochs", epoch=1000000000)
    original = Path.read_text
    reads = []
    def counted(path, *args, **kwargs):
        reads.append(path)
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", counted)
    results = ripgrep_search("matchingword", kb_store.storage_dir, max_results=300)
    assert len(results) == 1
    assert len(reads) <= 1


@pytest.mark.skipif(not RG_AVAILABLE, reason="ripgrep unavailable")
def test_scoped_rg_prunes_files_before_search(tmp_path, monkeypatch):
    import hyperkb.search as module
    filenames = ["target.exact.md", "target.exact.child.md", "target.exactly.md", "other.notes.md"]
    for name in filenames:
        (tmp_path / name).write_text(">>> 1000\nmatchingword\n<<<\n", encoding="utf-8")
    calls = []
    emitted_files = set()
    original_run = module.subprocess.run
    def record_run(cmd, **kwargs):
        calls.append(cmd)
        result = original_run(cmd, **kwargs)
        for line in result.stdout.splitlines():
            event = json.loads(line)
            if event["type"] == "match":
                emitted_files.add(Path(event["data"]["path"]["text"]).name)
        return result
    monkeypatch.setattr(module.subprocess, "run", record_run)
    results = ripgrep_search("matchingword", tmp_path, domain="target.exact")
    assert emitted_files == {"target.exact.md", "target.exact.child.md"}
    assert {result.file_name for result in results} == {"target.exact", "target.exact.child"}
    assert "*.md" not in calls[0]
    assert "-B" not in calls[0]
    assert "--before-context" not in calls[0]


@pytest.mark.skipif(not RG_AVAILABLE, reason="ripgrep unavailable")
def test_scoped_rg_treats_glob_metacharacters_literally(tmp_path):
    for name in ["target[ab].md", "target[ab].child.md", "targeta.md", "targetb.child.md"]:
        (tmp_path / name).write_text(">>> 1000\nmatchingword\n<<<\n", encoding="utf-8")
    results = ripgrep_search("matchingword", tmp_path, domain="target[ab]")
    assert {result.file_name for result in results} == {"target[ab]", "target[ab].child"}


@pytest.mark.skipif(not RG_AVAILABLE, reason="ripgrep unavailable")
def test_rg_option_like_query_is_a_literal_pattern(tmp_path, monkeypatch):
    import hyperkb.search as module
    (tmp_path / "test.flags.md").write_text(">>> 1000\nDocument the --version option here.\n<<<\n", encoding="utf-8")
    calls = []
    original_run = module.subprocess.run
    def record_run(cmd, **kwargs):
        calls.append(cmd)
        return original_run(cmd, **kwargs)
    monkeypatch.setattr(module.subprocess, "run", record_run)
    results = ripgrep_search("--version", tmp_path)
    assert len(results) == 1
    assert results[0].epoch == 1000
    assert "--version" in results[0].content
    assert calls[0][-3:] == ["--", "--version", str(tmp_path)]
    assert "--fixed-strings" in calls[0][:calls[0].index("--")]


@pytest.mark.skipif(not RG_AVAILABLE, reason="ripgrep unavailable")
def test_filename_search_terminates_options_before_root(tmp_path, monkeypatch):
    import hyperkb.search as module
    (tmp_path / "test.flags.md").write_text("notes", encoding="utf-8")
    calls = []
    original_run = module.subprocess.run
    def record_run(cmd, **kwargs):
        calls.append(cmd)
        return original_run(cmd, **kwargs)
    monkeypatch.setattr(module.subprocess, "run", record_run)
    assert module.ripgrep_search_filenames("flags", tmp_path) == ["test.flags"]
    assert calls[0][-2:] == ["--", str(tmp_path)]
