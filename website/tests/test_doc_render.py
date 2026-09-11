"""Focused checks for the shared Markdown documentation renderer."""

import unittest

from scripts.doc_render import render_document


class DocumentRendererTests(unittest.TestCase):
    def render(self, text, source='docs/SETUP.md', routes=None):
        return render_document(text, source, routes or {
            'docs/SETUP.md': 'guides/setup/index.html',
            'docs/OTHER.md': 'guides/other.html',
            'DISCLAIMER.md': 'disclaimer.html',
        })

    def test_title_toc_tables_lists_and_code(self):
        result = self.render('''# A **useful** `guide`

## Setup

| Name | State |
| --- | --- |
| Node | private |

- First
- Second

```python
# Not a title
## Not a section
print("<safe>")
```

### Read more

    # Indented code stays code

Use `mesh_status`.
''')
        self.assertEqual(result['title'], 'A useful guide')
        self.assertNotIn('<h1', result['body_html'])
        self.assertIn('<span class="doc-title-anchor" id="a-useful-guide"></span>', result['body_html'])
        self.assertEqual(result['toc'], [
            {'id': 'setup', 'title': 'Setup', 'level': 2},
            {'id': 'read-more', 'title': 'Read more', 'level': 3},
        ])
        for fragment in ('<table>', '<ul>', 'class="language-python"',
                         '# Not a title', '## Not a section', '&lt;safe&gt;',
                         '# Indented code stays code', '<code>mesh_status</code>'):
            self.assertIn(fragment, result['body_html'])

    def test_links_are_resolved_from_source_then_rebased_to_output(self):
        result = self.render('''# Links

[Other](OTHER.md?view=full#access-decisions)
[Root](/DISCLAIMER.md#scope)
[Local](#links)
[Raw](../llm.txt)
[Unmapped](KEEP.md#raw)
![Diagram](../images/a%20b.svg)
[Source](https://github.com/example/project/blob/main/agentmesh/node.py#L20)
''')
        for target in ('../other.html?view=full#access-decisions',
                       '/disclaimer.html#scope', '#links', '../../llm.txt',
                       '../../docs/KEEP.md#raw', '../../images/a%20b.svg',
                       'https://github.com/example/project/blob/main/agentmesh/node.py#L20'):
            self.assertIn('"' + target + '"', result['body_html'])

    def test_directory_routes_and_parent_paths(self):
        result = self.render('# Guide\n\n[Other](../OTHER.md#section)',
            source='docs/nested/GUIDE.md', routes={
                'docs/nested/GUIDE.md': '/learn/nested/',
                'docs/OTHER.md': '/learn/other/',
            })
        self.assertIn('href="../other/#section"', result['body_html'])

        root = self.render('# Guide\n\n[Home](../../README.md) [Files](../../downloads/)',
            source='docs/nested/GUIDE.md', routes={
                'docs/nested/GUIDE.md': '/learn/nested/',
                'README.md': '/',
            })
        self.assertIn('href="../../"', root['body_html'])
        self.assertIn('href="../../downloads/"', root['body_html'])

    def test_github_ids_preserve_duplicates_unicode_and_punctuation(self):
        result = self.render('''# Guide

## Setup
## Setup
### Setup-1
## Setup
## Café — 中文 &amp; `mesh_name`
#### Deep detail
''')
        self.assertEqual([item['id'] for item in result['toc']],
                         ['setup', 'setup-1', 'setup-1-1', 'setup-2', 'café--中文--mesh_name'])
        self.assertEqual(result['toc'][-1]['title'], 'Café — 中文 & mesh_name')
        self.assertIn('<h4 id="deep-detail">', result['body_html'])

    def test_raw_html_is_escaped_and_dangerous_urls_are_not_active(self):
        result = self.render('''# Safe

<script>alert("bad")</script>

<iframe src="https://example.com"></iframe>

[Bad](javascript:alert(1))
![Bad image](data:text/html,evil)

Inline <b onclick="bad()">markup</b> stays text.
''')
        output = result['body_html']
        self.assertNotIn('<script', output)
        self.assertNotIn('<iframe', output)
        self.assertNotIn('<b onclick', output)
        self.assertNotIn('href="javascript:', output)
        self.assertNotIn('src="data:', output)
        self.assertIn('&lt;script&gt;', output)
        self.assertIn('&lt;b onclick=', output)

    def test_external_autolinks_bare_urls_and_code(self):
        result = self.render('''# Links

<https://example.com/first>

Read https://example.com/path_(one).

`https://example.com/code` remains code.

[Already linked](https://example.com/original)
''')
        output = result['body_html']
        for url in ('https://example.com/first', 'https://example.com/path_(one)',
                    'https://example.com/original'):
            self.assertIn('href="' + url + '"', output)
        self.assertIn('<code>https://example.com/code</code>', output)
        self.assertEqual(output.count('<a '), 3)

    def test_setext_title_entities_and_later_h1(self):
        result = self.render('''AT&amp;T and `<peer>`
=======================

## A ![node](../node.svg)

# Another title
''')
        self.assertEqual(result['title'], 'AT&T and <peer>')
        self.assertIn('id="att-and-peer"', result['body_html'])
        self.assertEqual(result['toc'], [{'id': 'a-node', 'title': 'A node', 'level': 2}])
        self.assertIn('<h1 id="another-title">Another title</h1>', result['body_html'])

    def test_each_render_resets_heading_ids(self):
        first = self.render('# Guide\n\n## Repeat\n\n## Repeat')
        second = self.render('# Guide\n\n## Repeat\n\n## Repeat')
        self.assertEqual(first, second)

    def test_explicit_external_route_preserves_fragment(self):
        result = self.render('# Guide\n\n[Maintainer](PRIVATE.md#build)', routes={
            'docs/SETUP.md': 'docs/setup.html',
            'docs/PRIVATE.md': 'https://github.com/example/repo/blob/main/docs/PRIVATE.md',
        })
        self.assertIn('href="https://github.com/example/repo/blob/main/docs/PRIVATE.md#build"', result['body_html'])


if __name__ == '__main__':
    unittest.main()
