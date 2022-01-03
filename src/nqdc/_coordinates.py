import logging
import re
import html

import pandas as pd
from lxml import etree

from nqdc import _utils

_LOG = logging.getLogger(__name__)

_TRIPLET = r"""(?ix)
    ^{prefix}
    [\[({{]?
    \s*(?P<x>{x_pat})\s*(?:,|;|\s)\s*
    (?P<y>{y_pat})\s*(?:,|;|\s)\s*
    (?P<z>{z_pat})\s*
    [])}}]?
    {postfix}$"""

_COORD_HEAD_TRIPLET = _TRIPLET.format(
    x_pat="x", y_pat="y", z_pat="z", prefix=".*?", postfix=".*"
)

_COORD_HEAD_NAME = r"""
    (?ix)
    .*
    (?:
    coordinate
    |
    \bcoords?\b
    |
    talairach
    |
    \btal\b
    |
    \bmni\b
    |
    location)
    .*
    """

_NB_PATTERN = r"[-+]?\s*(\d+(\.\d*)?|\.\d+)(\s*[eE][-+]?\d+)?"

_COORD_DATA_TRIPLET = _TRIPLET.format(
    x_pat=_NB_PATTERN,
    y_pat=_NB_PATTERN,
    z_pat=_NB_PATTERN,
    prefix=r"\s*",
    postfix=r"\s*",
)


class CoordinateExtractor:
    def __init__(self):
        self._stylesheet = _utils.load_stylesheet("table_extraction.xsl")

    def __call__(self, article):
        return _extract_coordinates_from_article(article, self._stylesheet)


def _extract_coordinates_from_article(article, stylesheet):
    try:
        transformed = stylesheet(article)
    except Exception:
        _LOG.exception(f"failed to transform article: {stylesheet.error_log}")
        return None
    try:
        coordinates = _extract_coordinates_from_article_tables(transformed)
        return coordinates
    except Exception:
        _LOG.exception("failed to extract coords from article")
        return None


def _extract_coordinates_from_article_tables(article_tables):
    all_coordinates = []
    for i, table in enumerate(article_tables.iterfind("//extracted-table")):
        try:
            table_id = table.find("table-id").text
            table_label = table.find("table-label").text
            table_data = pd.read_html(
                _map_chars(
                    etree.tostring(
                        table.find("transformed-table//{*}table")
                    ).decode("utf-8")
                ),
                thousands=None,
                flavor="lxml",
            )[0]
        except Exception:
            _LOG.debug(f"Failed to read table # {i}")
            continue
        try:
            coordinates = _extract_coordinates_from_table(table_data)
        except Exception:
            _LOG.exception(
                f"Failed to extract coordinates from table {table_id}"
            )
            continue
        coordinates["table_id"] = table_id
        coordinates["table_label"] = table_label
        all_coordinates.append(coordinates)
    if all_coordinates:
        return pd.concat(all_coordinates)
    return pd.DataFrame(columns=["x", "y", "z", "table_id", "table_label"])


def _map_chars(text):
    _char_map = {
        0x2212: "-",
        0x2796: "-",
        0x2013: "-",
        0xFE63: "-",
        0xFF0D: "-",
        0xFF0B: "+",
    }
    return html.unescape(text).translate(_char_map)


def _extract_coordinates_from_table(table, copy=False):
    if copy:
        table = table.copy()
    if isinstance(table.columns, pd.MultiIndex):
        table.columns = table.columns.get_level_values(-1)
    table.columns = list(map(str, table.columns))
    table = _expand_all_xyz_cols(table)
    xyz_indices = _find_xyz(table.columns)
    if not xyz_indices:
        return pd.DataFrame(columns=["x", "y", "z"])
    table = table.fillna("")
    result = pd.concat(
        [
            pd.DataFrame(
                table.iloc[:, index].values, columns=["x", "y", "z"], dtype=str
            )
            for index in xyz_indices
        ]
    )
    for column in result:
        result[column] = _to_numeric(result[column])
    result.dropna(inplace=True)
    result.reset_index(inplace=True, drop=True)
    return result


def _expand_all_xyz_cols(table, start=0):
    for pos in range(start, table.shape[1]):
        if re.match(_COORD_HEAD_TRIPLET, table.columns[pos]) or re.match(
            _COORD_HEAD_NAME, table.columns[pos]
        ):
            expanded, start = _expand_xyz_column(table, pos)
            return _expand_all_xyz_cols(expanded, start=start)
    return table


def _expand_xyz_column(table, pos):
    xyz = table.iloc[:, pos]
    as_numbers = _to_numeric(xyz)
    n_numbers = as_numbers.notnull().sum()
    coord_columns = xyz.apply(_split_xyz)
    n_triplets = (coord_columns != "").all(axis=1).sum()
    if n_numbers > n_triplets:
        return table, pos + 1
    expanded = table.iloc[
        :, list(range(pos)) + list(range(pos + 1, table.shape[1]))
    ]
    expanded.insert(pos, "z", coord_columns.iloc[:, 2], allow_duplicates=True)
    expanded.insert(pos, "y", coord_columns.iloc[:, 1], allow_duplicates=True)
    expanded.insert(pos, "x", coord_columns.iloc[:, 0], allow_duplicates=True)
    return expanded, pos + 3


def _split_xyz(triplet):
    found = re.match(_COORD_DATA_TRIPLET, str(triplet))
    if found is None:
        return pd.Series(["", "", ""])
    return pd.Series([found.group("x"), found.group("y"), found.group("z")])


def _to_numeric(series):
    return pd.to_numeric(
        series.apply(
            lambda x: re.sub(r"(\+|\-)\s+", r"\g<1>", x)
            if isinstance(x, str)
            else x
        ),
        errors="coerce",
    )


def _find_xyz(tr):
    found = []
    pos = 0
    while pos < len(tr) - 2:
        if (
            re.search(r"\bx\b", tr[pos], re.I)
            and re.search(r"\by\b", tr[pos + 1], re.I)
            and re.search(r"\bz\b", tr[pos + 2], re.I)
        ) or (
            re.match(_COORD_HEAD_NAME, tr[pos])
            and re.match(_COORD_HEAD_NAME, tr[pos + 1])
            and re.match(_COORD_HEAD_NAME, tr[pos + 2])
            and not re.search(r"\bx\b", tr[pos + 1], re.I)
        ):
            found.append([pos, pos + 1, pos + 2])
            pos += 2
        pos += 1
    return found
