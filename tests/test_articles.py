import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from lxml import etree

from nqdc import ExitCode, _articles, _download, _utils


@pytest.mark.parametrize("n_jobs", [1, 3])
def test_extract_articles(n_jobs, tmp_path, entrez_mock, monkeypatch):
    monkeypatch.setattr(_articles, "_LOG_PERIOD", 2)
    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path
    )
    assert code == ExitCode.COMPLETED
    articles_dir = Path(f"{download_dir}-articles")
    created_dir, code = _articles.extract_articles(
        download_dir, articles_dir, n_jobs=n_jobs
    )
    assert created_dir == articles_dir
    assert code == ExitCode.COMPLETED
    assert len(list(articles_dir.glob("**/article.xml"))) == 7

    # check tables
    tables_dir = articles_dir.joinpath("19d", "pmcid_9057060", "tables")
    assert tables_dir.is_dir()
    assert tables_dir.joinpath("tables.xml").is_file()
    coords = pd.read_csv(tables_dir.joinpath("table_000.csv"))
    assert (coords.values == [[10, 20, 30], [-10, -20, -30]]).all()

    # check does not repeat completed extraction
    with patch("nqdc._articles._extract_from_articleset") as mock:
        created_dir, code = _articles.extract_articles(
            download_dir, articles_dir
        )
        assert len(mock.mock_calls) == 0
        assert code == ExitCode.COMPLETED

    # check returns 1 if download incomplete
    info_file = download_dir.joinpath("info.json")
    info = json.loads(info_file.read_text("utf-8"))
    info["is_complete"] = False
    info_file.write_text(json.dumps(info), "utf-8")
    created_dir, code = _articles.extract_articles(download_dir)
    assert created_dir == tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc", "articles"
    )
    assert code == ExitCode.INCOMPLETE
    info_file.unlink()
    _, code = _articles.extract_articles(download_dir)
    assert code == ExitCode.INCOMPLETE


def test_extract_tables_content_multiple_header_rows(tmp_path):
    tables = etree.XML(
        """<extracted-tables-set>
    <pmcid>123</pmcid>
    <extracted-table>
    <table-id />
    <table-label />
    <table-caption />
    <transformed-table>
    <table>
        <thead>
            <tr><td>Task 1</td></tr>
            <tr><td>x, y, z</td></tr>
            <tr><td>Something</td></tr>
        </thead>
        <tbody>
            <tr><td>-10,-15,+68 </td></tr>
        </tbody>
    </table>
    </transformed-table>
    </extracted-table>
    </extracted-tables-set>
    """
    )
    _articles._extract_tables_content(tables, tmp_path)
    table_info, table_data = _utils.read_article_table(
        tmp_path.joinpath("table_000_info.json")
    )
    assert table_data.shape == (1, 1)
    assert table_data.columns.nlevels == 3
    assert table_info["n_header_rows"] == 3


def test_inline_elems(tmp_path):
    """Check that elements inside table cells match the default template

    (are replaced by their value)
    """
    article = etree.XML(
        """<article><front><article-meta>
        <article-id pub-id-type="pmc">123</article-id></article-meta></front>
        <body>
        <table-wrap>
    <table>
        <thead>
            <tr><td>x, y, z</td></tr>
        </thead>
        <tbody>
            <tr><td><inline-formula>-</inline-formula>10,-15,+68 </td></tr>
        </tbody>
    </table>
        </table-wrap>
    </body>
        </article>"""
    )
    tmp_path.joinpath("article.xml").write_bytes(
        etree.tostring(article, encoding="UTF-8", xml_declaration=True)
    )
    _articles._extract_tables(tmp_path)
    _, table_data = _utils.read_article_table(
        tmp_path.joinpath("tables", "table_000_info.json")
    )
    assert table_data.iloc[0, 0] == "-10,-15,+68"


def test_table_extraction_failure(tmp_path):
    """Failing to extract tables from an article is allowed.

    (does not raise an exception)
    """
    _articles._extract_tables(tmp_path)


def test_table_content_extraction_failure(tmp_path):
    """If one table fails the rest can still be extracted."""
    xml = etree.XML(
        """<extracted-tables-set>
    <extracted-table/>
    <extracted-table>
    <table-id />
    <table-label />
    <table-caption />
    <transformed-table>
    <table>
    <tr><th>A</th><th>B</th></tr>
    <tr><td>1</td><td>2</td></tr>
    <tr><td>10</td><td>20</td></tr>
    </table>
    </transformed-table>
    </extracted-table>
    </extracted-tables-set>
    """
    )
    _articles._extract_tables_content(xml, tmp_path)
    assert len(list(tmp_path.glob("table*info.json"))) == 1
