"""Extracting stereotactic coordinates from XML articles."""
import logging
import pathlib
import re
from typing import Any, List, Sequence, Tuple

import numpy as np
import pandas as pd
from lxml import etree
from scipy import stats

from pubget import _utils
from pubget._typing import Extractor

_LOG = logging.getLogger(__name__)

_CHAR_MAP = {
    0x2212: "-",
    0x2796: "-",
    0x2013: "-",
    0xFE63: "-",
    0xFF0D: "-",
    0xFF0B: "+",
}

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


class CoordinateExtractor(Extractor):
    """Extracting coordinates from articles."""

    fields = _COORD_FIELDS
    name = "coordinates"

    def extract(
        self, article: etree.ElementTree, article_dir: pathlib.Path
    ) -> pd.DataFrame:
        coords = _extract_coordinates_from_article_dir(article_dir)
        return coords.loc[:, self.fields]


def _extract_coordinates_from_article_dir(
    article_dir: pathlib.Path,
) -> pd.DataFrame:
    pmcid = _utils.get_pmcid_from_article_dir(article_dir)
    all_coordinates = []
    for table_info, table_data in _utils.get_tables_from_article_dir(
        article_dir
    ):
        try:
            coordinates = _extract_coordinates_from_table(table_data)
        except Exception:
            _LOG.debug(
                "Failed to extract coordinates from table "
                f"{table_info['table_id']} "
                f"in article pmcid {pmcid}"
            )
            continue
        coordinates["pmcid"] = pmcid
        coordinates["table_id"] = table_info["table_id"]
        coordinates["table_label"] = table_info["table_label"]
        all_coordinates.append(coordinates)
    if all_coordinates:
        return pd.concat(all_coordinates)
    return pd.DataFrame(columns=_COORD_FIELDS)


def _extract_coordinates_from_table(table: pd.DataFrame) -> pd.DataFrame:
    table = table.applymap(
        lambda x: x if not isinstance(x, str) else x.translate(_CHAR_MAP)
    )
    if isinstance(table.columns, pd.MultiIndex):
        table.columns = [
            " ".join(map(str, level_values)) for level_values in table.columns
        ]
    table.columns = list(map(str, table.columns))
    table = _expand_all_xyz_cols(table)
    xyz_indices = _find_xyz(table.columns)
    if not xyz_indices:
        return pd.DataFrame(columns=["x", "y", "z"])
    table = table.fillna("")
    result = pd.concat(
        [
            pd.DataFrame(
                table.iloc[:, list(idx)].values,
                columns=["x", "y", "z"],
                dtype=str,
            )
            for idx in xyz_indices
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


def _is_coord_triplet(columns: Sequence[str]) -> bool:
    if (
        re.search(r"\bx\b", columns[0], re.I)
        and re.search(r"\by\b", columns[1], re.I)
        and re.search(r"\bz\b", columns[2], re.I)
    ):
        return True
    if (
        re.match(_COORD_HEAD_NAME, columns[0])
        and re.match(_COORD_HEAD_NAME, columns[1])
        and re.match(_COORD_HEAD_NAME, columns[2])
        and not re.search(r"\bx\b", columns[1], re.I)
    ):
        return True
    return False


def _find_xyz(table_columns: Sequence[str]) -> List[Tuple[int, int, int]]:
    found = []
    pos = 0
    while pos < len(table_columns) - 2:
        if _is_coord_triplet(table_columns[pos : pos + 3]):
            found.append((pos, pos + 1, pos + 2))
            pos += 3
        else:
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
