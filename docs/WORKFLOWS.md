# Everyday workflows

The useful habit is simple: retrieve before repeating work, and save what changed your understanding. The calls below are examples for an MCP client.

## Start with a briefing

```python
hkb_session(action="briefing", domain="myproject")
hkb_session(action="anchor", topics="authentication, deployment")
```

A briefing collects recent knowledge and outstanding work. Anchors bias later retrieval toward the current session's topics. They are local to the client process; they are not a shared team setting.

## Save a finding and its consequence

Create a topic once, then append entries to it:

```python
hkb_add(create_file=True, to="app.performance",
        description="Measured performance and operating limits.",
        keywords=["latency", "performance", "limits"])
hkb_add(to="app.performance",
        content="@type: finding\n@tags: connection-pool\nIncreasing the pool beyond 20 exhausted the worker budget during our load test.")
```

Include the conditions behind a measurement. Avoid turning one environment's result into a universal rule.

## Find a home for new knowledge

```python
hkb_search(mode="check", query="Postgres connection pool capacity decision")
```

Inspect routing candidates. Use `hkb_add(to="the.chosen-topic", content="...")` when you know the intended file. With `to` omitted, HyperKB can route high-confidence matches, but low-confidence results need a deliberate choice.

## Carry a decision into the next session

```python
hkb_search(query="connection pool", type="decision", domain="infra")
hkb_context(topic="postgres connection pool", max_tokens=2500)
hkb_show(name="infra.postgres")
```

Context packing returns complete selected entries by default. If an entry is too large to fit, increase the budget or read the file directly. `depth="shallow"` explicitly requests short previews.

## Track a task

```python
hkb_task(action="create", file="tasks.myproject",
         title="Investigate connection pool limit",
         description="Measure worker use under realistic load.")
hkb_task(action="list", domain="tasks")
```

Use the epoch returned by creation when updating:

```python
hkb_task(action="update", file="tasks.myproject", epoch=1789142400,
         status="completed", note="Recorded findings in app.performance.")
```

The epoch above is illustrative; replace it with the actual returned value.

## Keep a correction visible

```python
hkb_update(file="infra.postgres", epoch=1789142400,
           set_status="superseded", add_tags="reviewed")
hkb_add(to="infra.postgres",
        content="@type: decision\nThe new worker budget supports a different pool limit. Supersedes [[infra.postgres#1789142400]].")
```

A new linked entry preserves the reasoning behind a change. Direct content replacement is also supported when an amendment is the intended action. Archiving moves an entry to its archive companion; ordinary search excludes archive files by default.

## Save a focused view

```python
hkb_view(action="set", name="performance-work",
         files=["infra.postgres", "app.performance"])
hkb_session(action="briefing", view="performance-work", focus="capacity")
```

A view is a named collection of files. Unlike session anchors, views are stored in the knowledge base and can be reused across sessions.

## Finish with a review

```python
hkb_session(action="review", after="4h", domain="myproject")
hkb_health(checks="all")
```

Review what was recorded and capture any missing rationale before the conversation ends. See [search & context](SEARCH.md) for time filters and retrieval behavior.
