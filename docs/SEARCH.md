# Search & context

HyperKB searches locally using ripgrep and SQLite FTS5/BM25. Entry search does not require an embedding model or an external API.

## Pick a search mode

```python
hkb_search(query="connection pool", domain="infra")
hkb_search(query="postgres", mode="bm25")
hkb_search(query="pool", mode="rg")
```

| Mode | Behavior |
| --- | --- |
| `hybrid` | Combines ripgrep and BM25 candidates, deduplicates entries, then applies ranking signals |
| `rg` | Uses ripgrep text matching; a multiword query matches any of its words |
| `bm25` | Uses indexed keyword matching and stemming |
| `recent` | Returns a recent timeline rather than a keyword search |
| `check` | Suggests topic files for new content |

The public MCP interface does not expose a regex switch. Do not assume that `mode="rg"` interprets a phrase or a regular expression as a strict pattern.

## Filter before choosing a result

```python
hkb_search(query="connection pool", domain="infra", type="decision",
           status="active", author="editor-a", after="2w", top=10)
```

Status, type, author, and hostname filters apply across search sources. Domain scopes a dotted namespace. `include_archived=True` includes archive companions. Supported time inputs include epoch timestamps, durations such as `30m`, `4h`, `2d`, and `1w`, and ISO dates such as `2026-09-11`.

Use `offset` and `top` for search pagination. The default result count is 10; the MCP interface bounds the requested page size. Results contain entry content, a snippet, file, epoch, provenance, and scoring metadata.

## Understand ranking

Text relevance is the starting point. Recency, entry type, status, importance, and session anchors influence ordering. A high score indicates relevance under those signals; it does not verify a fact or replace reading the original rationale.

High-weight entries and decisions are exempt from the staleness penalty. Resolved or superseded entries can still be useful historical context, so they are dampened rather than automatically removed unless you filter them out.

## Pack complete context

```python
hkb_context(topic="postgres connection limits", max_tokens=3000)
```

The default `packed` mode retrieves candidate entries, ranks them for the topic, and includes complete entries that fit. Matching-line snippets are not substituted for full content. `depth="shallow"` requests the first 200 characters instead.

`tokens_used` is an estimate based on the complete JSON response, including metadata, file summaries, and truncation notices. It is not a count from a specific model tokenizer. The estimator is kept within `tokens_budget`; actual model token counts can differ, especially for code and non-English text. Very small budgets may not fit even the mandatory response metadata.

Check `truncated` and the returned entries before assuming all relevant knowledge was included. Truncation notices themselves are bounded by the budget.

## Suggest files or follow a story

```python
hkb_context(mode="suggest", topic="refactor authentication", top=5)
hkb_context(mode="narrative", topic="database migration", depth="1")
```

Suggestions identify files to inspect. Narrative mode follows entry links for up to two hops and can order the results chronologically. A link's presence is not evidence that the linked claim is current.

## Keep the index current

Writes through HyperKB update the index. If you edit Markdown using another program, run `hkb reindex`. The index is rebuilt from a coordinated snapshot, and readers do not share its partial rebuild transaction. See [maintenance & recovery](OPERATIONS.md).
