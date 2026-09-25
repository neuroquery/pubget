import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from lxml import etree

from pubget import _articles, _text, _utils

_TABLE = """<table-wrap>
  <label>Table 1</label>
  <caption><p>Peak coordinates.</p></caption>
  <table>
    <thead>
      <tr><th/><th colspan="2">MNI</th></tr>
      <tr><th>Region</th><th>x</th><th>y</th></tr>
    </thead>
    <tbody>
      <tr><td rowspan="2">IFG</td><td>-42</td><td>18</td></tr>
      <tr><td>-40</td><td>20</td></tr>
    </tbody>
  </table>
  <table-wrap-foot>
    <fn><p>IFG = inferior frontal gyrus.</p></fn>
  </table-wrap-foot>
</table-wrap>"""

_TABLE_TEXT = """Table 1
\tMNI\tMNI
Region\tx\ty
IFG\t-42\t18
IFG\t-40\t20
IFG = inferior frontal gyrus.
"""


def _make_article(body: str) -> bytes:
    article = f"""<article>
        <front><article-meta>
          <article-id pub-id-type='pmc'>123</article-id>
        </article-meta></front>
        <body>{body}</body>
      </article>"""
    return article.encode("UTF-8")


@pytest.fixture(autouse=True)
def clear_stylesheet_cache():
    """Avoid leaking mocked stylesheets between tests."""
    _utils.load_stylesheet.cache_clear()
    yield
    _utils.load_stylesheet.cache_clear()


@pytest.fixture
def article_with_table(tmp_path):
    """An article dir with tables, as created by `extract_articles`."""
    article_dir = tmp_path.joinpath("pmcid_123")
    article_dir.mkdir()
    article_dir.joinpath("article.xml").write_bytes(
        _make_article(f"<p>Results.</p>{_TABLE}")
    )
    _articles._extract_tables(article_dir)
    return article_dir


def test_text_extractor_transform_failure(monkeypatch):
    """Transforming the article to extract text is allowed to fail."""
    extractor = _text.TextExtractor()
    monkeypatch.setattr(
        etree, "XSLT", Mock(return_value=Mock(side_effect=ValueError))
    )
    assert extractor.extract(Mock(), Mock(), {}) == {}


def test_text_extractor_preserves_xref_text_by_default():
    extractor = _text.TextExtractor()
    article = etree.fromstring(
        _make_article(
            "<p>Example ( <xref ref-type='bibr'>Doe et al.</xref> ) test.</p>"
        )
    )
    result = extractor.extract(article, Path("."), {})
    body = " ".join(result["body"].split())
    assert "Doe et al." in body
    assert "( )" not in body


def test_text_extractor_strips_xref_text_when_disabled():
    extractor = _text.TextExtractor(preserve_cross_references=False)
    article = etree.fromstring(
        _make_article(
            "<p>Example ( <xref ref-type='bibr'>Doe et al.</xref> ) test.</p>"
        )
    )
    result = extractor.extract(article, Path("."), {})
    body = " ".join(result["body"].split())
    assert "Doe et al." not in body
    assert "( )" in body


def test_text_extractor_omits_tables_by_default(article_with_table):
    extractor = _text.TextExtractor()
    article = etree.parse(str(article_with_table.joinpath("article.xml")))
    result = extractor.extract(article, article_with_table, {})
    assert "Peak coordinates." in result["body"]
    assert "-42" not in result["body"]


def test_text_extractor_keeps_tables_when_enabled(article_with_table):
    """Header rows, merged cells and the footer are kept with the table."""
    extractor = _text.TextExtractor(keep_tables=True)
    article = etree.parse(str(article_with_table.joinpath("article.xml")))
    result = extractor.extract(article, article_with_table, {})
    body = result["body"]
    assert _TABLE_TEXT in body
    # the table is inserted where it appears in the article
    assert body.index("Peak coordinates.") < body.index(_TABLE_TEXT)
    assert "pubget-table" not in body


def test_table_text_reproduces_table_csv(tmp_path):
    """Cells are not passed through pandas' type inference a second time."""
    tables_dir = tmp_path.joinpath("tables")
    tables_dir.mkdir()
    tables_dir.joinpath("table_000.csv").write_text(
        "Unnamed: 0,Subject\n,0012\n", "UTF-8"
    )
    tables_dir.joinpath("table_000_info.json").write_text(
        json.dumps(
            {
                "table_label": None,
                "table_foot": None,
                "n_header_rows": 1,
                "table_data_file": "table_000.csv",
            }
        ),
        "UTF-8",
    )
    assert _text._load_tables(tmp_path) == {0: "\tSubject\n\t0012\n"}


def test_text_extractor_drops_placeholders_of_missing_tables(
    article_with_table,
):
    """Tables that `extract_articles` failed to parse leave no placeholder."""
    for table_file in article_with_table.joinpath("tables").glob("table_*"):
        table_file.unlink()
    extractor = _text.TextExtractor(keep_tables=True)
    article = etree.parse(str(article_with_table.joinpath("article.xml")))
    result = extractor.extract(article, article_with_table, {})
    assert "Peak coordinates." in result["body"]
    assert "pubget-table" not in result["body"]


def test_text_extractor_reports_unreadable_tables(article_with_table, caplog):
    """A table that cannot be read does not prevent extracting the text."""
    article_with_table.joinpath("tables", "table_000.csv").write_text("")
    extractor = _text.TextExtractor(keep_tables=True)
    article = etree.parse(str(article_with_table.joinpath("article.xml")))
    result = extractor.extract(article, article_with_table, {})
    assert "Peak coordinates." in result["body"]
    assert "pubget-table" not in result["body"]
    assert "failed to read table" in caplog.text
