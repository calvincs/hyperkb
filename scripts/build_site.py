#!/usr/bin/env python3
"""Build the public GitHub Pages tree from an explicit source allowlist."""
from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
import posixpath
import sys

try:
    from .doc_render import render_document
except ImportError:
    from doc_render import render_document

ROOT = Path(__file__).resolve().parents[1]
SITE_URL = 'https://hyperkb.com'
REPO_URL = 'https://github.com/calvincs/hyperkb'
# Source, human route, navigation label, description. Maintainer documents are
# deliberately excluded. Only these snapshots and ASSETS can be published.
DOCUMENTS = [
    ('README.md', 'docs/overview.html', 'Project overview', 'What HyperKB does and where to begin.'),
    ('docs/GETTING_STARTED.md', 'docs/getting-started.html', 'Getting started', 'Install, connect a client, and save your first entry.'),
    ('docs/UNDERSTANDING.md', 'docs/understanding.html', 'How it works', 'Topics, entries, links, and the storage model.'),
    ('docs/MCP.md', 'docs/mcp.html', 'MCP & clients', 'Connect multiple clients and understand tool results.'),
    ('docs/WORKFLOWS.md', 'docs/workflows.html', 'Everyday workflows', 'Briefings, decisions, tasks, and focused context.'),
    ('docs/SEARCH.md', 'docs/search.html', 'Search & context', 'Search modes, filters, ranking, and estimated budgets.'),
    ('docs/SYNC.md', 'docs/sync.html', 'Sync between machines', 'Set up a destination, upgrade safely, and resolve conflicts.'),
    ('docs/OPERATIONS.md', 'docs/operations.html', 'Maintenance & recovery', 'Diagnose, reindex, archive, and back up your knowledge.'),
    ('docs/CONFIGURATION.md', 'docs/configuration.html', 'Configuration', 'Settings, optional capabilities, and credential handling.'),
    ('SKILL.md', 'docs/agent-reference.html', 'Agent reference', 'A practical reference for agents using the ten MCP tools.'),
    ('CHANGELOG.md', 'docs/changelog.html', 'Changelog', 'Current changes and historical releases.'),
    ('LICENSE', 'license.html', 'MIT license', 'The terms for using and contributing to HyperKB.'),
]
ASSETS = ('index.html', 'style.css', 'site.js', 'docs.css', 'docs.js', 'mark.svg', 'CNAME')


def esc(value):
    return html.escape(str(value), quote=True)


def relative(target, route):
    return posixpath.relpath(target, posixpath.dirname(route) or '.')


def shell(route, title, description, body, toc=(), source=None):
    href = lambda target: esc(relative(target, route))
    navigation = ''.join(
        f'<a href="{href(target)}"' + (' aria-current="page"' if target == route else '') +
        f'>{esc(label)}</a>' for _, target, label, _ in DOCUMENTS)
    contents = ''.join(f'<a class="toc-level-{item["level"]}" href="#{esc(item["id"])}">{esc(item["title"])}</a>' for item in toc)
    source_links = (f'<a href="{href(source)}">Read Markdown ↗</a><a href="{REPO_URL}/blob/main/{esc(source)}">View source ↗</a>' if source else '')
    return f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)} · HyperKB</title><meta name="description" content="{esc(description)}">
<link rel="canonical" href="{SITE_URL}/{route}"><link rel="icon" href="{href('mark.svg')}" type="image/svg+xml">
<meta name="theme-color" content="#050810"><link rel="stylesheet" href="{href('style.css')}"><link rel="stylesheet" href="{href('docs.css')}">
<script src="{href('docs.js')}" defer></script></head><body class="docs-page">
<a class="skip" href="#main">Skip to content</a>
<header class="site-header wrap"><a class="brand" href="{href('index.html')}" aria-label="HyperKB home"><span class="brand-prompt" aria-hidden="true">▸</span><span>HyperKB</span></a><nav aria-label="Main navigation"><a href="{href('docs/index.html')}" aria-current="{('page' if route == 'docs/index.html' else 'false')}">Documentation</a><a href="{href('llms.txt')}">For agents</a><a href="{REPO_URL}">GitHub ↗</a></nav></header>
<div class="docs-layout wrap"><aside class="docs-sidebar"><a class="docs-home" href="{href('docs/index.html')}">DOCUMENTATION</a><label class="doc-search-label" for="doc-search">Find a guide</label><input id="doc-search" type="search" placeholder="Filter guides…" autocomplete="off" aria-controls="doc-nav" hidden><noscript><p class="docs-muted">Browse all guides below.</p></noscript><button class="doc-nav-toggle" type="button" aria-controls="doc-nav" aria-expanded="true" hidden>Browse guides</button><nav id="doc-nav" aria-label="Documentation navigation">{navigation}</nav><p id="search-status" class="docs-muted" role="status" aria-live="polite"></p><a class="machine-link" href="{href('agent.json')}">Machine-readable index ↗</a></aside>
<main id="main" class="docs-main"><div class="doc-kicker">HYPERKB / DOCUMENTATION</div><h1>{esc(title)}</h1><p class="doc-description">{esc(description)}</p><div class="doc-source-links">{source_links}</div><article class="doc-prose">{body}</article><footer class="doc-end"><a href="{href('docs/index.html')}">← All guides</a><a href="{REPO_URL}/issues">Suggest an improvement ↗</a></footer></main>
<aside class="docs-toc" aria-label="On this page"><span>ON THIS PAGE</span>{contents or '<p>One short page.</p>'}</aside></div>
<footer class="site-footer wrap"><p>Knowledge that stays with you.</p><nav aria-label="Footer navigation"><a href="{href('index.html')}">Home</a><a href="{href('license.html')}">MIT license</a><a href="{href('llms.txt')}">llms.txt</a></nav></footer></body></html>'''


def build_files():
    files = {name: (ROOT / 'website' / name).read_bytes() for name in ASSETS}
    routes = {source: route for source, route, _, _ in DOCUMENTS}
    # README's maintainer link stays on GitHub; implementation material is not
    # copied into the public site merely because a document happens to link it.
    routes['docs/WEBSITE.md'] = REPO_URL + '/blob/main/docs/WEBSITE.md'
    routes['CONTRIBUTING.md'] = REPO_URL + '/blob/main/CONTRIBUTING.md'
    index = []
    for source, route, label, description in DOCUMENTS:
        text = (ROOT / source).read_text(encoding='utf-8')
        files[source] = text.encode()
        if source == 'LICENSE':
            document = {'title': 'MIT license', 'body_html': '<pre>' + esc(text) + '</pre>', 'toc': []}
        else:
            if source == 'SKILL.md' and text.startswith('---\n'):
                text = text.split('---\n', 2)[2].lstrip()
            document = render_document(text, source, routes)
        files[route] = shell(route, document['title'], description, document['body_html'], document['toc'], source).encode()
        index.append({'title': label, 'description': description, 'url': SITE_URL + '/' + route, 'source': SITE_URL + '/' + source})
    cards = ''.join(f'<a class="doc-card" href="{esc(relative(route, "docs/index.html"))}"><span>{number:02d}</span><h2>{esc(label)} ↗</h2><p>{esc(description)}</p></a>' for number, (_, route, label, description) in enumerate(DOCUMENTS[:-1], 1))
    intro = '<div class="docs-intro"><p>Start with one useful memory. Learn the storage model, connect your clients, then build a habit of recording what the next session will need.</p><a class="button primary" href="getting-started.html">Start the setup guide ↗</a></div><div class="doc-card-grid">' + cards + '</div>'
    files['docs/index.html'] = shell('docs/index.html', 'A guide to lasting context.', 'Practical guides for people. Plain-text references for agents.', intro).encode()
    files['404.html'] = shell('404.html', 'That page has moved.', 'Your next useful page is close by.', '<p><a href="/">Return home</a> or <a href="/docs/index.html">browse the documentation</a>.</p>').replace('<head>', '<head><base href="/">').encode()
    for source in ('llm.txt', 'llms.txt'):
        files[source] = (ROOT / source).read_bytes()
    files['agent.json'] = (json.dumps({'name': 'HyperKB', 'description': 'Markdown knowledge for MCP clients, with local hybrid search and optional S3 synchronization.', 'website': SITE_URL, 'repository': REPO_URL, 'license': 'MIT', 'transport': 'local stdio', 'instructions': SITE_URL + '/llm.txt', 'index': SITE_URL + '/llms.txt', 'documents': index}, indent=2) + '\n').encode()
    pages = sorted(name for name in files if name.endswith('.html') and name != '404.html')
    files['sitemap.xml'] = ('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + ''.join(f'  <url><loc>{SITE_URL}/{name}</loc></url>\n' for name in pages) + '</urlset>\n').encode()
    files['robots.txt'] = f'User-agent: *\nAllow: /\nSitemap: {SITE_URL}/sitemap.xml\n'.encode()
    files['.nojekyll'] = b''
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / '.site-build')
    parser.add_argument('--check', action='store_true', help='Verify output exactly matches canonical sources without writing')
    args = parser.parse_args()
    expected = build_files()
    output = args.output.resolve()
    if output == ROOT or output in ROOT.parents or output == ROOT / 'website':
        parser.error('Output must be a dedicated build directory, not a source directory')
    if args.check:
        actual = {p.relative_to(output).as_posix(): p.read_bytes() for p in output.rglob('*') if p.is_file()}
        drift = sorted(name for name in actual.keys() | expected.keys() if actual.get(name) != expected.get(name))
        if drift:
            print('Site drift: ' + ', '.join(drift), file=sys.stderr)
            return 1
    else:
        # Refuse to mix unknown files into a publishable tree. Do not delete
        # files in a user-supplied output directory.
        unknown = [p for p in output.rglob('*') if p.is_file() and p.relative_to(output).as_posix() not in expected]
        if unknown:
            parser.error('Output contains files outside the public allowlist; use an empty directory')
        for name, content in expected.items():
            destination = output / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
    print(f'{"Verified" if args.check else "Built"} {len(expected)} public files in {output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
