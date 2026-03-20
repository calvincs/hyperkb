# Changelog

## v1.8.0
### Context-Aware Retrieval
- **`@weight` metadata**: New entry importance field (`high`, `normal`, `low`). High-weight entries get 1.15x score boost and are exempt from staleness penalty.
- **Staleness-aware scoring**: Active entries older than 2x recency half-life are penalized (floor 0.7). Decisions and high-weight entries exempt. New config: `recency_half_life_days` (default 180).
- **Expanded status scoring**: `pending` (1.08x boost), `in_progress` (1.05x boost), `completed` (0.88x dampen), `cancelled` (0.65x dampen), `blocked` (0.95x dampen).
- **`hkb_context`**: Token-budgeted retrieval — searches and packs entries into a token budget using greedy knapsack with type priority re-ranking.
- **`hkb_context_suggest`**: Proactive file suggestions for a task, with link-graph expansion to surface connected files.
- **`hkb_anchor`**: Session-only topic bias — 1.5x soft score boost on matching files across search, briefing, recent, context, and suggest. Not a hard filter.
- **`hkb_narrative`**: Cross-file chronological story reconstruction via search + wiki-link graph traversal (depth 0–2).
- **`hkb_briefing` focus/view**: New `focus` param uses search for topic-ranked briefings with relevance scores. New `view` param scopes briefings to named file groupings.
- **`hkb_view_set` / `hkb_view_list`**: Named context views — lightweight file groupings stored as entries in `views.workspaces`.

### Documentation
- README.md rewritten with full tool reference (30 tools), entry metadata docs, scoring pipeline, config key table, and context-aware retrieval guide.
- SKILL.md updated with new tools, workflow progression templates, and `@weight` guidance.
- MCP SERVER_INSTRUCTIONS updated with context retrieval guidance.

### Internal
- New DB column: `weight` on entries table (schema migration).
- New DB method: `get_entries_by_keys()` for batch entry fetching.
- New store methods: `build_context()`, `suggest_context()`, `build_narrative()`, `set_view()`, `get_view()`, `list_views()`.
- `AppContext` gains `anchors` and `anchor_files` for session state.
- 84 new tests in `test_context_retrieval.py`. Total: 735 tests.

## v1.7.1
- Sync worker now performs an immediate sync on startup instead of waiting a full interval
- Default sync check interval reduced from 5 minutes to 1 minute
- `hkb update` now compares installed package version against latest git tag, not local vs remote tags

## v1.6.0
- Added `hkb update` CLI for manual upgrade workflow
- Switched to setuptools-scm for dynamic versioning
- MCP server identity now includes version string
- Added release.sh helper script

## v1.5.0
- Initial versioned release

## v1.7.0
- Always reinstall on update for version sync
