"""Render authored Markdown as safe, linked documentation fragments.

Requires Python-Markdown with its bundled fenced_code, tables and sane_lists
extensions. Sources and routes are site-root-relative POSIX paths. A route may
also start with '/' or end in '/' for an index page. Raw HTML in authored text
is escaped; generated Markdown links, images, tables and code remain supported.
"""

from __future__ import annotations

import html
import posixpath
import unicodedata
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

import markdown
from markdown.extensions.toc import stashedHTML2text
from markdown.inlinepatterns import InlineProcessor
from markdown.treeprocessors import Treeprocessor


def _source_path(path: str) -> str:
    return posixpath.normpath(path.lstrip('/'))


def _heading_slug(title: str) -> str:
    """GitHub heading rules: lowercase, remove punctuation, space -> hyphen.

    Preserve Unicode letters/numbers/marks, underscores and existing hyphens.
    In particular, repeated spaces/hyphens are not collapsed, and Unicode text
    is not transliterated. Duplicate suffixes are assigned by the tree processor.
    """
    return ''.join(
        character for character in title.lower()
        if character in ' _-' or unicodedata.category(character)[0] in 'LNM'
    ).replace(' ', '-')


def _plain_heading(element: ET.Element, md: markdown.Markdown) -> str:
    parts = []

    def visit(node):
        if node.text:
            parts.append(node.text)
        for child in node:
            if child.tag == 'img':
                parts.append(child.get('alt', ''))
            else:
                visit(child)
            if child.tail:
                parts.append(child.tail)

    visit(element)
    # Markdown stashes character entities as well as fenced blocks. Resolve
    # entity placeholders so neither titles nor heading IDs contain internals.
    return html.unescape(stashedHTML2text(''.join(parts), md, strip_entities=False)).strip()


def _rebase_url(value: str, source: str, routes: dict[str, str], *, image=False) -> str | None:
    value = html.unescape(value).strip()
    parsed = urlsplit(value)
    allowed = {'http', 'https'} if image else {'http', 'https', 'mailto', 'tel', 'ftp', 'ftps'}
    if parsed.scheme:
        return value if parsed.scheme.lower() in allowed else None
    if parsed.netloc or not parsed.path:
        return value

    authored = unquote(parsed.path)
    if authored.startswith('/'):
        target = _source_path(authored)
    else:
        target = _source_path(posixpath.join(posixpath.dirname(source), authored))
    destination = routes.get(target, target)
    if urlsplit(destination).scheme in {'http', 'https'}:
        mapped = urlsplit(destination)
        return urlunsplit((mapped.scheme, mapped.netloc, mapped.path,
                           parsed.query or mapped.query, parsed.fragment or mapped.fragment))
    if target not in routes and authored.endswith('/') and not destination.endswith('/'):
        destination += '/'
    # Mapped documents get their human route. Unmapped .md, raw instructions,
    # downloads and images retain their original public target after rebasing.
    if parsed.path.startswith('/'):
        path = '/' + destination.lstrip('/')
    else:
        current = routes.get(source, source)
        directory = posixpath.dirname(current.lstrip('/'))
        path = posixpath.relpath(destination.lstrip('/') or '.', directory or '.')
        if destination.endswith('/') and not path.endswith('/'):
            path += '/'
    path = quote(path, safe="/:@!$&'()*+,;=-._~%")
    return urlunsplit(('', '', path, parsed.query, parsed.fragment))


class _BareLinks(InlineProcessor):
    """Add bare HTTP(S) links without parsing or rewriting Markdown syntax."""

    ANCESTOR_EXCLUDES = ('a', 'code', 'pre')

    def handleMatch(self, match, data):
        value = match.group(0).rstrip('.,;:!?')
        for opening, closing in (('(', ')'), ('[', ']'), ('{', '}')):
            while value.endswith(closing) and value.count(closing) > value.count(opening):
                value = value[:-1]
        element = ET.Element('a', href=value)
        element.text = value
        return element, match.start(0), match.start(0) + len(value)


class _DocumentTree(Treeprocessor):
    def __init__(self, md, source, routes):
        super().__init__(md)
        self.source = source
        self.routes = routes
        self.title = ''
        self.toc = []

    def run(self, root):
        used = set()
        title_found = False
        for element in root.iter():
            if element.tag in {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}:
                title = _plain_heading(element, self.md)
                base = _heading_slug(title)
                slug = base
                number = 0
                while slug in used:
                    number += 1
                    slug = f'{base}-{number}'
                used.add(slug)
                level = int(element.tag[1])
                element.set('id', slug)
                if level == 1 and not title_found:
                    self.title = title
                    title_found = True
                    # The shared page shell owns the visible H1. Keep its
                    # original fragment target without duplicating that title.
                    tail = element.tail
                    element.clear()
                    element.tag = 'span'
                    element.set('id', slug)
                    element.set('class', 'doc-title-anchor')
                    element.tail = tail
                elif level in (2, 3):
                    self.toc.append({'id': slug, 'title': title, 'level': level})
            for attribute in ('href', 'src'):
                if attribute not in element.attrib:
                    continue
                try:
                    value = _rebase_url(element.get(attribute), self.source, self.routes,
                                        image=attribute == 'src')
                except ValueError:
                    value = None
                if value is None:
                    del element.attrib[attribute]
                else:
                    element.set(attribute, value)


def render_document(text: str, source: str, routes: dict[str, str]) -> dict:
    """Return a plain title, HTML body (without its first H1), and H2/H3 TOC."""
    source = _source_path(source)
    routes = {_source_path(key): value for key, value in routes.items()}
    md = markdown.Markdown(extensions=['fenced_code', 'tables', 'sane_lists'],
                           output_format='html')
    # Keep Markdown's entity and autolink handling, but do not admit arbitrary
    # authored script/style/iframe/event-handler HTML into the generated page.
    md.preprocessors.deregister('html_block')
    md.inlinePatterns.deregister('html')
    md.inlinePatterns.register(_BareLinks(r'https?://[^\s<>"\x00-\x1f]+', md), 'bare_http_links', 105)
    processor = _DocumentTree(md, source, routes)
    md.treeprocessors.register(processor, 'document_routes_and_headings', -1)
    body = md.convert(text)
    title = processor.title or posixpath.splitext(posixpath.basename(source))[0].replace('_', ' ')
    return {'title': title, 'body_html': body, 'toc': processor.toc}
