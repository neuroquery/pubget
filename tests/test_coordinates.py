import re
from unittest.mock import Mock

from lxml import etree
import numpy as np
import pandas as pd

from nqdc import _coordinates


def _example_table(values_start=20):
    a = np.asarray(
        [
            f"[{x}, - {2*x}, + 20.8]"
            for x in range(values_start, values_start + 15)
        ]
    ).reshape((-1, 5))
    df = pd.DataFrame(
        a, columns=["a", "x,y,z", "a", "x,y,z", "MNI coordinates"]
    )
    return df


def test_triplet():
    """check what is matched as coordinate triplets in single column"""
    head_triplet = re.compile(_coordinates._COORD_HEAD_TRIPLET)
    head_triplet.match("(x, y, z)") is not None
    head_triplet.match("[x Y z ]") is not None
    head_triplet.match("{ x;y;z }") is not None
    head_triplet.match("x, y, z.1") is not None
    head_triplet.match("x, y, z") is not None
    head_triplet.match("[x, y, z)") is not None
    head_triplet.match("(x, y; z)") is not None
    head_triplet.match("peak(x, y; z)") is not None
    head_triplet.match("(x y, z)") is not None
    assert head_triplet.match("(x, y)") is None
    assert head_triplet.match("region") is None

    data_triplet = re.compile(_coordinates._COORD_DATA_TRIPLET)
    match = data_triplet.match("3e8, -12.8, .9")
    assert (match.group("x"), match.group("y"), match.group("z")) == (
        "3e8",
        "-12.8",
        ".9",
    )
    match = data_triplet.match("3e8 - 12.8,+ .9")
    assert (match.group("x"), match.group("y"), match.group("z")) == (
        "3e8",
        "- 12.8",
        "+ .9",
    )
    match = data_triplet.match("3, -12.8, .9]")
    assert match is not None
    non_match = data_triplet.match("BA(3, 12, 9) ")
    assert non_match is None
    non_match = data_triplet.match("3, 12")
    assert non_match is None


def test_split_xyz():
    assert (
        _coordinates._split_xyz("-3; 1e4; .77") == ["-3", "1e4", ".77"]
    ).all()
    assert (
        _coordinates._split_xyz("( -3 , 1e4 , .77 )") == ["-3", "1e4", ".77"]
    ).all()
    assert (_coordinates._split_xyz("-") == ["", "", ""]).all()


def test_expand_xyz_column():
    table = _example_table()
    print(table.head())
    expanded, start = _coordinates._expand_xyz_column(table, 2)
    assert start == 5
    assert expanded.shape[1] == 7
    assert (expanded.iloc[:, 2] == ["22", "27", "32"]).all()
    assert (expanded.iloc[:, 3] == ["- 44", "- 54", "- 64"]).all()
    assert (expanded.iloc[:, 4] == "+ 20.8").all()
    table = pd.DataFrame({"xyz": [1, 2, 3, "456"]})
    expanded, start = _coordinates._expand_xyz_column(table, 0)
    assert start == 1


def test_expand_all_xyz_cols():
    table = _example_table()
    expanded = _coordinates._expand_all_xyz_cols(table)
    print(expanded)
    assert (
        expanded.columns
        == ["a", "x", "y", "z", "a", "x", "y", "z", "x", "y", "z"]
    ).all()
    assert (
        (_coordinates._expand_all_xyz_cols(expanded) == expanded).all().all()
    )


def test_find_xyz():
    found = _coordinates._find_xyz(
        [
            "a",
            "x",
            "y",
            "z",
            "Z",
            "b",
            "x.1",
            "y.1",
            "z.1",
            "Z.1",
            "x.2",
            "y.2",
            "a",
            "location ",
            "location ",
            "location ",
        ]
    )
    assert found == [(1, 2, 3), (6, 7, 8), (13, 14, 15)]


def test_table_to_coordinates():
    table = _example_table()
    table.columns = pd.MultiIndex.from_arrays(
        [["left", "left", "right", "right", ""], table.columns]
    )
    table.iloc[1, 1] = "--"
    coords = _coordinates._extract_coordinates_from_table(table)
    assert (coords.x == [21, 31, 23, 28, 33, 24, 29, 34]).all()
    assert (
        _coordinates._extract_coordinates_from_table(_example_table(0)).shape[
            0
        ]
        == 0
    )


def test_check_empty_table():
    table = pd.DataFrame(columns=list("xyz"))
    assert _coordinates._check_table(table)


def test_multiple_header_rows():
    """column names that indicate coordinates
    are found even if buried in the middle of multiple header rows."""
    tables = etree.XML(
        """<extracted-tables-set>
    <pmcid>123</pmcid>
    <extracted-table>
    <table-id />
    <table-label />
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
    coords = _coordinates._extract_coordinates_from_article_tables(tables)
    assert (
        coords.loc[:, ["x", "y", "z"]].values.ravel() == [-10, -15, 68]
    ).all()


def test_inline_elems():
    """Check that:
    - docbook xhtml conversion is applied
    - elements inside table cells match the default template (are replaced by
      their value)
    """
    tables = etree.XML(
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
    coords = _coordinates.CoordinateExtractor().extract(tables)
    assert (
        coords.loc[:, ["x", "y", "z"]].values.ravel() == [-10, -15, 68]
    ).all()


def test_char_mapping():
    """unicode characters that look like + or - are correctly mapped"""
    tables = etree.XML(
        """<extracted-tables-set>
    <pmcid>123</pmcid>
    <extracted-table>
    <table-id />
    <table-label />
    <transformed-table>
    <table>
    <tr><td>x, y, z</td></tr>
    <tr><td>−10,➖ 15,＋68 </td></tr>
    </table>
    </transformed-table>
    </extracted-table>
    </extracted-tables-set>
    """
    )
    coords = _coordinates._extract_coordinates_from_article_tables(tables)
    assert (
        coords.loc[:, ["x", "y", "z"]].values.ravel() == [-10, -15, 68]
    ).all()


def test_coordinate_extraction_failures(monkeypatch):
    """Bad articles for which extration fails don't raise an exception."""
    _coordinates._extract_coordinates_from_article(
        Mock(), Mock(side_effect=ValueError)
    )
    xml_mock = Mock()
    xml_mock.find.side_effect = ValueError
    stylesheet_mock = Mock()
    stylesheet_mock.side_effect = xml_mock
    _coordinates._extract_coordinates_from_article(Mock(), stylesheet_mock)
    tables = etree.XML(
        """<extracted-tables-set>
    <pmcid>123</pmcid>
    <extracted-table/>
    <extracted-table>
    <table-id />
    <table-label />
    <transformed-table>
    <table>
    <tr><td>x, y, z</td></tr>
    <tr><td>−10,➖ 15,＋68 </td></tr>
    </table>
    </transformed-table>
    </extracted-table>
    </extracted-tables-set>
    """
    )
    monkeypatch.setattr(
        _coordinates,
        "_extract_coordinates_from_table",
        Mock(side_effect=ValueError),
    )
    _coordinates._extract_coordinates_from_article_tables(tables)
