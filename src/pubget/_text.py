"""Extracting text from XML articles."""
import json
import logging
import pathlib
import re
from typing import Dict, Union

import pandas as pd
from lxml import etree

from pubget import _utils
from pubget._typing import Extractor, Records

_LOG = logging.getLogger(__name__)

# Left in the text by 'text_extraction.xsl' at the position of each table when
# tables are kept, and by the files written for each table by
# `pubget.extract_articles`. In both cases the number is the table's rank in
# the article, which is how the two are matched.
_TABLE_PLACEHOLDER = re.compile(r"\[pubget-table-(\d+)\]")
_TABLE_INFO_FILE = re.compile(r"table_(\d+)_info\.json")
# The name pandas gives to a column whose header cell is empty.
_UNNAMED_COLUMN = re.compile(r"^Unnamed: \d+(_level_\d+)?$")


class TextExtractor(Extractor):
    """Extracting text from XML articles."""

    fields = ("pmcid", "title", "keywords", "abstract", "body")
    name = "text"

    def __init__(
        self,
        preserve_cross_references: bool = True,
        keep_tables: bool = False,
    ) -> None:
        self.preserve_cross_references = preserve_cross_references
        self.keep_tables = keep_tables

    def extract(
        self,
        article: etree.ElementTree,
        article_dir: pathlib.Path,
        previous_extractors_output: Dict[str, Records],
    ) -> Dict[str, Union[str, int]]:
        del previous_extractors_output
        result: Dict[str, Union[str, int]] = {}
        # Stylesheet is not parsed in init because lxml.XSLT cannot be pickled
        # so that would prevent the extractor from being passed to
        # multiprocessing map. Parsing is cached.
        stylesheet = _utils.load_stylesheet("text_extraction.xsl")
        try:
            transformed = stylesheet(
                article,
                **{
                    "preserve-crossrefs": etree.XSLT.strparam(
                        "true" if self.preserve_cross_references else "false"
                    ),
                    "keep-tables": etree.XSLT.strparam(
                        "true" if self.keep_tables else "false"
                    ),
                },
            )
        except Exception:
            _LOG.exception(
                f"failed to transform article: {stylesheet.error_log}"
            )
            return result
        for part_name in self.fields:
            elem = transformed.find(part_name)
            result[part_name] = elem.text
        if self.keep_tables and result["body"]:
            result["body"] = _insert_tables(str(result["body"]), article_dir)
        result["pmcid"] = _utils.get_pmcid(article)
        return result


def _insert_tables(body: str, article_dir: pathlib.Path) -> str:
    """Replace the placeholders left in `body` by the tables' contents.

    Placeholders left for tables that `pubget.extract_articles` did not manage
    to parse are removed.
    """
    tables = _load_tables(article_dir)
    return _TABLE_PLACEHOLDER.sub(
        lambda match: tables.get(int(match.group(1)), ""), body
    )


def _load_tables(article_dir: pathlib.Path) -> Dict[int, str]:
    """Read the tables extracted from an article by `extract_articles`.

    Keys are the tables' rank in the article. Tables that cannot be read are
    left out.
    """
    tables = {}
    for info_file in _utils.get_table_info_files_from_article_dir(article_dir):
        match = _TABLE_INFO_FILE.match(info_file.name)
        assert match is not None
        try:
            tables[int(match.group(1))] = _format_table(info_file)
        except Exception:
            _LOG.exception(f"failed to read table {info_file}")
    return tables


def _format_table(info_file: pathlib.Path) -> str:
    """Render one of an article's extracted tables as tab-separated values.

    The label and the footer are added because, unlike the caption, they are
    not part of the extracted text.
    """
    table_info = json.loads(info_file.read_text("UTF-8"))
    table_data = _read_table_data(
        info_file.with_name(table_info["table_data_file"]),
        table_info["n_header_rows"],
    )
    grid: str = table_data.to_csv(
        sep="\t", index=False, header=False, lineterminator="\n"
    )
    parts = [
        table_info["table_label"],
        grid.strip("\n"),
        table_info["table_foot"],
    ]
    table_text = "\n".join(filter(None, parts))
    return f"{table_text}\n"


def _read_table_data(
    table_csv: pathlib.Path, n_header_rows: int
) -> pd.DataFrame:
    """Read a table's CSV file, header rows included, as text.

    Unlike `_utils.read_article_table`, this does not attempt to convert the
    cells: they are reproduced exactly as `extract_articles` wrote them,
    rather than passed through pandas' type inference a second time. The
    header rows are read as regular rows so they keep their original position
    and the placeholder names pandas gives to unnamed columns can be removed.
    """
    table_data = pd.read_csv(
        table_csv, header=None, dtype=str, keep_default_na=False
    )
    header = table_data.iloc[:n_header_rows]
    table_data.iloc[:n_header_rows] = header.replace(
        _UNNAMED_COLUMN, "", regex=True
    )
    return table_data
