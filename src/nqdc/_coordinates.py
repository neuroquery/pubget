import logging
import re
import html
from typing import Tuple, Any, Sequence, List

import numpy as np
from scipy import stats
import pandas as pd
from lxml import etree

from nqdc import _utils
from nqdc._typing import BaseExtractor


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

_COORD_HEAD_NAME = r"""(?ix)
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
_COORD_FIELDS = ("pmcid", "table_id", "table_label", "x", "y", "z")


class CoordinateExtractor(BaseExtractor):
    fields = _COORD_FIELDS
    name = "coordinates"

    def __init__(self) -> None:
        self._stylesheet = _utils.load_stylesheet("table_extraction.xsl")

    def extract(self, article: etree.ElementTree) -> pd.DataFrame:
        coords = _extract_coordinates_from_article(article, self._stylesheet)
        return coords.loc[:, self.fields]


def _extract_coordinates_from_article(
    article: etree.ElementTree, stylesheet: etree.XSLT
) -> pd.DataFrame:
    try:
        transformed = stylesheet(article)
    except Exception:
        _LOG.exception(f"failed to transform article: {stylesheet.error_log}")
        return pd.DataFrame(columns=_COORD_FIELDS)
    try:
        coordinates = _extract_coordinates_from_article_tables(transformed)
        return coordinates
    except Exception:
        _LOG.exception("failed to extract coords from article")
        return pd.DataFrame(columns=_COORD_FIELDS)


def _extract_coordinates_from_article_tables(
    article_tables: etree.Element,
) -> pd.DataFrame:
    pmcid = int(article_tables.find("pmcid").text)
    all_coordinates = []
    for i, table in enumerate(article_tables.iterfind("extracted-table")):
        try:
            table_id = table.find("table-id").text
            table_label = table.find("table-label").text
            kwargs = {"header": 0} if not table.find("th") else {}
            table_data = pd.read_html(
                _map_chars(
                    etree.tostring(
                        table.find("transformed-table//{*}table")
                    ).decode("utf-8"),
                ),
                thousands=None,
                flavor="lxml",
                **kwargs,
            )[0]
        except Exception:
            _LOG.debug(f"Failed to read table # {i} in article pmcid {pmcid}")
            continue
        try:
            coordinates = _extract_coordinates_from_table(table_data)
        except Exception:
            _LOG.exception(
                f"Failed to extract coordinates from table {table_id} "
                f"in article pmcid {pmcid}"
            )
            continue
        coordinates["pmcid"] = pmcid
        coordinates["table_id"] = table_id
        coordinates["table_label"] = table_label
        all_coordinates.append(coordinates)
    if all_coordinates:
        return pd.concat(all_coordinates)
    return pd.DataFrame(columns=_COORD_FIELDS)


def _map_chars(text: str) -> str:
    _char_map = {
        0x2212: "-",
        0x2796: "-",
        0x2013: "-",
        0xFE63: "-",
        0xFF0D: "-",
        0xFF0B: "+",
    }
    return html.unescape(text).translate(_char_map)


def _extract_coordinates_from_table(table: pd.DataFrame) -> pd.DataFrame:
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
    if not _check_table(result):
        return pd.DataFrame(columns=["x", "y", "z"])
    result = _filter_coordinates(result)
    result.reset_index(inplace=True, drop=True)
    return result


def _expand_all_xyz_cols(table: pd.DataFrame, start: int = 0) -> pd.DataFrame:
    for pos in range(start, table.shape[1]):
        if re.match(_COORD_HEAD_TRIPLET, table.columns[pos]) or re.match(
            _COORD_HEAD_NAME, table.columns[pos]
        ):
            expanded, start = _expand_xyz_column(table, pos)
            return _expand_all_xyz_cols(expanded, start=start)
    return table


def _expand_xyz_column(
    table: pd.DataFrame, pos: int
) -> Tuple[pd.DataFrame, int]:
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


def _split_xyz(triplet: Any) -> pd.Series:
    found = re.match(_COORD_DATA_TRIPLET, str(triplet))
    if found is None:
        return pd.Series(["", "", ""])
    return pd.Series([found.group("x"), found.group("y"), found.group("z")])


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.apply(
            lambda x: re.sub(r"(\+|\-)\s+", r"\g<1>", x)
            if isinstance(x, str)
            else x
        ),
        errors="coerce",
    )


def _find_xyz(tr: Sequence[str]) -> List[List[int]]:
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


def _filter_coordinates(coordinates: pd.DataFrame) -> pd.DataFrame:
    xyz = coordinates.loc[:, ("x", "y", "z")]
    outside_brain = (xyz.abs() >= 150).any(axis=1)
    not_coord = (-1 <= xyz).all(axis=1) & (xyz <= 1).all(axis=1)
    max_2_positions = (xyz.round(2) == xyz).all(axis=1)
    filtered = coordinates[~outside_brain & ~not_coord & max_2_positions]
    return filtered


def _check_table(values: np.ndarray, tol: float = -400) -> bool:
    if not values.shape[0]:
        return True
    distrib = stats.multivariate_normal(mean=[0, 0, 0], cov=1.5)
    avg_ll = float(distrib.logpdf(values).mean())
    return avg_ll < tol
