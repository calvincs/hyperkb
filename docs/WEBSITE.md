# Maintaining the GitHub Pages site

The website follows the documentation pattern used by Agent Mesh: canonical Markdown in the main branch, generated human pages, original Markdown snapshots, and small machine-readable entry points. GitHub Pages remains the hosting target at `hyperkb.com`.

## Edit the sources

- `website/index.html`, `style.css`, and `site.js`: landing page and its illustrative memory walkthrough.
- `website/docs.css` and `docs.js`: responsive documentation shell, guide filter, and copy controls.
- `README.md`, `SKILL.md`, `CHANGELOG.md`, `LICENSE`, and the public guides in `docs/`: canonical content.
- `llm.txt` and `llms.txt`: short agent instructions and documentation index.
- `scripts/build_site.py`: explicit public allowlist, routes, and static page generation.

`CODE_REVIEW.md`, this maintainer guide, tests, build requirements, and application source are excluded from the published tree. An external link to a repository file does not publish a copy of that file. Add documents deliberately to `DOCUMENTS` when they belong in the public reference.

Do not edit generated output. Public pages and Markdown snapshots are built from the same sources so agents and people see the same documentation.

The visual identity comes from the original HyperKB site (`gh-pages` commit `c974955`): cyan and violet accents, deep navy panels, a subtle grid, Oxanium headings, Syne body text, and Space Mono labels. Shared colors and fonts live in `website/style.css`; `docs.css` uses those same tokens. Keep this theme consistent across the current layouts and interactive controls.

## Build and check

```bash
.venv/bin/pip install -r website/requirements.txt
.venv/bin/python scripts/build_site.py
.venv/bin/python scripts/check_site.py
.venv/bin/python scripts/build_site.py --check
.venv/bin/python -m pytest website/tests -q
python3 -m http.server 8766 --bind 127.0.0.1 --directory .site-build
```

The builder produces `.site-build/` by default. Use `--output /path/to/empty-directory` for a separate export. It refuses unknown files in the output directory rather than silently publishing or deleting them. The validator checks source parity, the allowlist, local links and fragments, unique IDs, sitemap coverage, and machine-readable routes.

The build dependency is Python-Markdown. Published pages require no application server or JavaScript framework. Without JavaScript, the documentation and walkthrough remain readable. Motion controls, reduced-motion support, guide filtering, and code copying progressively enhance the static content.

## Review before publishing

Check desktop and narrow layouts, documentation navigation, code examples, keyboard focus, guide filtering, and the walkthrough's pause and manual-step controls. Check reduced-motion behavior. Preserve the custom domain, MIT license, and existing analytics ID unless intentionally changing those settings.

The CI workflow validates the application and website and uploads the generated site as an artifact. It does not deploy. After release review, publish only the built allowlisted tree to the existing `gh-pages` branch; retain `CNAME` and `.nojekyll`. Review the branch diff before pushing. A website update describing new backend behavior should accompany the corresponding application release.

## Keep claims accurate

The walkthrough is an illustration, not a connected live MCP client. Avoid implying automatic capture of all conversations, model-specific token limits, a hosted shared HTTP server, vector search, or an external model requirement. Keep protocol warnings and migration instructions aligned with the implementation: local recording continues while remote sync is paused, and clients predating the checks need one coordinated upgrade.
