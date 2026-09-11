"""Publication boundaries and broken-reference detection for generated docs."""
from scripts.build_site import build_files
from scripts.check_site import check


def write_tree(path):
    for name, content in build_files().items():
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def test_public_tree_excludes_maintainer_material(tmp_path):
    write_tree(tmp_path)
    assert not check(tmp_path)
    assert not (tmp_path / 'CODE_REVIEW.md').exists()
    assert not (tmp_path / 'docs/WEBSITE.md').exists()
    assert not (tmp_path / 'website/requirements.txt').exists()
    assert not (tmp_path / 'hyperkb').exists()


def test_check_rejects_drift_broken_fragments_and_extra_files(tmp_path):
    write_tree(tmp_path)
    home = tmp_path / 'index.html'
    home.write_text(home.read_text().replace('</main>', '<a href="docs/search.html#missing-heading">Broken</a></main>'))
    (tmp_path / 'private.txt').write_text('not public')
    errors = check(tmp_path)
    assert any('missing fragment' in error for error in errors)
    assert any('drift: private.txt' in error for error in errors)
    assert any('drift: index.html' in error for error in errors)


def test_check_validates_machine_markdown_links(tmp_path):
    write_tree(tmp_path)
    source = tmp_path / 'llms.txt'
    source.write_text(source.read_text() + '\n[Missing guide](docs/MISSING.md)\n')
    assert any('llms.txt: missing target' in error for error in check(tmp_path))
