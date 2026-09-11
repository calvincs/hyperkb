# How HyperKB works

HyperKB is a local knowledge base for AI tools. Its job is to preserve useful information between sessions and retrieve it when it becomes relevant again.

## A memory is more than a saved chat

Store a concise finding, decision, or task rather than an entire transcript. Include enough context to make it useful later: what happened, what you chose, and why. A future session should be able to use the entry without reconstructing the original conversation.

For example, “pool limit: 20” is a setting. “Use a pool limit of 20 because higher limits exhaust our worker budget under load” preserves a decision.

## Files organize topics

Files use dotted namespaces instead of nested folders:

```text
infra.postgres.md
app.performance.md
ops.runbooks.md
```

Names normally have two to four segments, using lowercase letters, numbers, and hyphens. An `.archive` suffix is reserved for archive companions. Use a project name as the first segment when several projects share a knowledge base.

A file's YAML header describes its purpose, keywords, and related topics. Its body contains entries:

```markdown
---
name: infra.postgres
description: Postgres connection settings and operating decisions.
keywords: [postgres, connection, pool]
links: [app.performance]
created: "2026-09-11T12:00:00Z"
compacted: ""
---

>>> 1789142400
@type: decision
@weight: high
Use a pool limit of 20. Higher limits exhaust our worker budget.
See [[app.performance]].
<<<
```

## Markdown is the source of truth

The default layout is:

```text
~/.hkb/
  config.json       Settings
  index.db          Rebuildable SQLite index
  storage/          Markdown knowledge files
  sync/             Local synchronization state, when enabled
```

SQLite stores a searchable index, metadata, and links. It is not the only copy of your knowledge. `hkb reindex` rebuilds the index from Markdown. If you edit files with an external editor, rebuild before relying on indexed results; external editors do not participate in HyperKB's write coordination.

New entries are appended. Existing entries can be amended or archived through MCP. HyperKB therefore supports mutable entries; it does not promise an immutable or append-only audit log.

## Links preserve connections

- `[[app.performance]]` links to a topic.
- `[[app.performance#1789142400]]` links to a specific entry.
- `[[app.performance#latest]]` expresses a link to the latest entry.

Use explicit links when a decision depends on another finding. A link is a reference, not a guarantee that the target exists or that its content is correct.

## Multiple local clients

Each MCP client launches its own stdio process. The processes can share one knowledge base: coordinated file mutations protect writes, while SQLite provides the shared index. Each process has independent session anchors and a client-specific `HKB_SOURCE` label.

When sync is enabled, only one process owns background synchronization at a time. Other running processes can take over when that owner exits. This is local process coordination, not a remotely hosted MCP API. For another machine, configure [S3 synchronization](SYNC.md).

## Search and context

Entry search combines ripgrep matches and SQLite FTS5/BM25 ranking. Metadata, recency, and importance influence scores. File routing uses file metadata and filename matching. No vectors are required.

Search helps locate entries. Context packing chooses complete entries for a topic within a size estimate. Briefings help start a session, and narrative retrieval follows entry links to reconstruct related work. See [search & context](SEARCH.md).

## What HyperKB does not decide

HyperKB stores and retrieves what clients write. It does not independently verify a claim, resolve every contradictory decision, or guarantee that a retrieved entry is current. Treat retrieved text as information to evaluate, not instructions that override the current user's request.
