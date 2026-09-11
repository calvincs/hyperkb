#!/usr/bin/env python3
"""Validate generated public files, local links, fragments, metadata and drift."""
from __future__ import annotations
import argparse
from html.parser import HTMLParser
import json
from pathlib import Path
import posixpath
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET
try:
    from .build_site import ROOT, SITE_URL, build_files
    from .doc_render import render_document
except ImportError:
    from build_site import ROOT, SITE_URL, build_files
    from doc_render import render_document


class Page(HTMLParser):
    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.ids, self.links, self.duplicates = set(), [], []
        self.titles = self.h1s = 0
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            if attrs['id'] in self.ids:
                self.duplicates.append(attrs['id'])
            self.ids.add(attrs['id'])
        for name in ('href', 'src'):
            if attrs.get(name):
                self.links.append(attrs[name])
        self.titles += tag == 'title'
        self.h1s += tag == 'h1'


def check(output):
    expected = build_files()
    actual = {p.relative_to(output).as_posix(): p.read_bytes() for p in output.rglob('*') if p.is_file()}
    errors = [f'Source/output drift: {name}' for name in actual.keys() | expected.keys() if actual.get(name) != expected.get(name)]
    pages = {name: Page(data.decode()) for name, data in actual.items() if name.endswith('.html')}
    readable = dict(pages)
    for name, data in actual.items():
        if name.endswith('.md') or name == 'llms.txt':
            text = data.decode()
            if name == 'SKILL.md' and text.startswith('---\n'):
                text = text.split('---\n', 2)[2].lstrip()
            readable[name] = Page(render_document(text, name, {})['body_html'])
    for name, page in pages.items():
        if page.duplicates:
            errors.append(f'{name}: duplicate IDs {page.duplicates}')
        if page.titles != 1 or page.h1s != 1:
            errors.append(f'{name}: expected one title and one H1')
    for name, page in readable.items():
        for link in page.links:
            parsed = urlsplit(link)
            if parsed.scheme or parsed.netloc:
                continue
            path = unquote(parsed.path)
            target = (posixpath.normpath(path.lstrip('/')) if path.startswith('/') else
                      posixpath.normpath(posixpath.join(posixpath.dirname(name), path))) if path else name
            if path.endswith('/') or target == '.':
                target = target.rstrip('/') + '/index.html' if target != '.' else 'index.html'
            if target not in actual:
                errors.append(f'{name}: missing target {link} ({target})')
            elif parsed.fragment and target in readable and unquote(parsed.fragment) not in readable[target].ids:
                errors.append(f'{name}: missing fragment {link}')
    try:
        agent = json.loads(actual['agent.json'])
        for document in agent['documents']:
            for key in ('url', 'source'):
                if document[key].removeprefix(SITE_URL + '/') not in actual:
                    errors.append(f'agent.json: missing {document[key]}')
        locations = {node.text.removeprefix(SITE_URL + '/') for node in ET.fromstring(actual['sitemap.xml']).iter('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')}
        if locations != set(pages) - {'404.html'}:
            errors.append('Sitemap does not match public HTML pages')
    except (KeyError, ValueError, ET.ParseError) as exc:
        errors.append(f'Invalid machine-readable metadata: {exc}')
    if actual.get('CNAME', b'').strip() != b'hyperkb.com':
        errors.append('Custom domain changed')
    for error in sorted(errors):
        print(error)
    if not errors:
        print(f'Validated {len(pages)} HTML pages, {len(actual)} public files, links, fragments, sitemap and source parity')
    return errors


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / '.site-build')
    raise SystemExit(bool(check(parser.parse_args().output)))
