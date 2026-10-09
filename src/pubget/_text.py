"""Extracting text from XML articles."""
import json
import logging
import pathlib
import re
from typing import Dict, List, Tuple, Union

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
        keep_superscripts: bool = False,
    ) -> None:
        self.preserve_cross_references = preserve_cross_references
        self.keep_tables = keep_tables
        self.keep_superscripts = keep_superscripts

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
                    "keep-superscripts": etree.XSLT.strparam(
                        "true" if self.keep_superscripts else "false"
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

    Every table in the article ends up in the text. One that
    `pubget.extract_articles` did not manage to parse is rendered from
    `article.xml` instead, and one the body never places -- a table in
    `<floats-group>` -- is appended with its caption.
    """
    tables = _load_tables(article_dir)
    from_article = _article_tables(article_dir)
    placed = set()

    def _substitute(match: "re.Match[str]") -> str:
        rank = int(match.group(1))
        if rank in placed:
            return ""
        placed.add(rank)
        return tables.get(rank) or from_article.get(rank, ("", ""))[1]

    body = _TABLE_PLACEHOLDER.sub(_substitute, body)
    leftover = []
    for rank in sorted(set(tables) | set(from_article)):
        if rank in placed:
            continue
        caption, rendered = from_article.get(rank, ("", ""))
        table_text = tables.get(rank) or rendered
        if table_text:
            leftover.append("\n".join(filter(None, [caption, table_text])))
    if not leftover:
        return body
    return "\n".join([body.rstrip("\n") + "\n", *leftover])


def _article_tables(article_dir: pathlib.Path) -> Dict[int, Tuple[str, str]]:
    """Each `table-wrap` in `article.xml` as (caption, text), keyed by rank.

    The text has the same shape as `_format_table`'s, read straight from the
    XML, for the tables `extract_articles` could not parse. The rank counts
    every `table-wrap` in document order, as the placeholders and the table
    files both do.
    """
    article_file = article_dir.joinpath("article.xml")
    if not article_file.is_file():
        return {}
    try:
        article = etree.parse(str(article_file))
    except Exception:
        _LOG.exception(f"failed to parse {article_file}")
        return {}
    tables = {}
    for rank, wrap in enumerate(article.iter("{*}table-wrap")):
        label = _text_of(wrap.find("{*}label"))
        caption = _text_of(wrap.find("{*}caption"))
        rows = ["\t".join(row) for row in _table_rows(wrap)]
        foot = _text_of(wrap.find("{*}table-wrap-foot"))
        parts = [label, "\n".join(rows), foot]
        table_text = "\n".join(filter(None, parts))
        tables[rank] = (caption, f"{table_text}\n" if rows else "")
    return tables


def _text_of(elem: "etree._Element | None") -> str:
    if elem is None:
        return ""
    return " ".join("".join(elem.itertext()).split())


def _table_rows(wrap: etree._Element) -> List[List[str]]:
    """The cells of a table, with spanned cells repeated as pandas does."""
    rows: List[List[str]] = []
    spans: Dict[int, List] = {}  # column -> [text, rows still covered]
    for tr in wrap.iter("{*}tr"):
        row: List[str] = []

        def _fill_spans() -> None:
            while len(row) in spans:
                column = len(row)
                row.append(spans[column][0])
                spans[column][1] -= 1
                if spans[column][1] == 0:
                    del spans[column]

        for cell in tr:
            if etree.QName(cell).localname not in ("td", "th"):
                continue
            _fill_spans()
            text = _text_of(cell)
            rowspan = _span(cell, "rowspan")
            for _ in range(_span(cell, "colspan")):
                if rowspan > 1:
                    spans[len(row)] = [text, rowspan - 1]
                row.append(text)
        _fill_spans()
        if any(row):
            rows.append(row)
    return rows


def _span(cell: etree._Element, attribute: str) -> int:
    try:
        return max(1, min(int(cell.get(attribute, 1)), 100))
    except (TypeError, ValueError):
        return 1


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
