import logging
from pathlib import Path
import re
import html

import pandas as pd
from lxml import etree

from nqdc import utils

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


def extract_coordinates(articles_dir):
    articles_dir = Path(articles_dir)
    stylesheet = utils.load_stylesheet("table_extraction.xsl")
    all_coords = []
    n_articles, n_with_coords = 0, 0
    for subdir in sorted([f for f in articles_dir.glob("*") if f.is_dir()]):
        _LOG.info(f"Processing directory: {subdir.name}")
        for article_file in subdir.glob("pmcid_*.xml"):
            n_articles += 1
            _LOG.debug(
                f"In directory {subdir.name} "
                f"({int(subdir.name, 16) / int('fff', 16):.0%}), "
                f"processing article: {article_file.name}"
            )
            coords = _extract_coordinates_from_article(
                article_file, stylesheet
            )
            if coords is not None:
                coords_found = False
                for table_coords in coords:
                    if table_coords["coordinates"].shape[0]:
                        all_coords.append(table_coords["coordinates"])
                        coords_found = True
            n_with_coords += coords_found
            _LOG.info(
                f"Processed in total {n_articles} articles, {n_with_coords} "
                f"({n_with_coords / n_articles:.0%}) had coordinates"
            )
    return pd.concat(all_coords)


def _extract_coordinates_from_article(article_file, stylesheet):
    try:
        transformed = stylesheet(etree.parse(str(article_file)))
    except Exception:
        _LOG.exception(
            f"failed to transform {article_file.name}:"
            f" {stylesheet.error_log}"
        )
        return None
    try:
        coordinates = _extract_coordinates_from_article_tables(transformed)
        return coordinates
    except Exception:
        _LOG.exception(f"failed to extract coords from {article_file.name}")


def _extract_coordinates_from_article_tables(article_tables):
    all_table_data = []
    pmcid = article_tables.find("pmcid").text
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
            _LOG.debug(f"Failed to read table # {i} in PMCID {pmcid}")
            continue
        try:
            coordinates = _extract_coordinates_from_table(table_data)
        except Exception:
            _LOG.exception(
                f"Failed to extract coordinates from table {table_id}"
            )
            continue
        coordinates["pmcid"] = pmcid
        coordinates["table_id"] = table_id
        coordinates["table_label"] = table_label
        all_table_data.append(
            {
                "pmcid": pmcid,
                "table_id": table_id,
                "table_label": table_label,
                "table_data": table_data,
                "coordinates": coordinates,
            }
        )
    return all_table_data


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
