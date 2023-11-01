"""Extracting table info from article directories."""
import json
from pathlib import Path
from typing import Any, Dict

from lxml import etree
import pandas as pd

from pubget import _utils
from pubget._typing import Extractor, Records

_TABLE_FIELDS = (
    "pmcid",
    "table_id",
    "table_label",
    "table_caption",
    "table_foot",
    "n_header_rows",
    "table_data_file",
)


class TableInfoExtractor(Extractor):
    """Read table info JSON files so they can be assembled in a single CSV."""

    fields = _TABLE_FIELDS
    name = "tables"

    def extract(
        self,
        article: etree.ElementTree,
        article_dir: Path,
        previous_extractors_output: Dict[str, Records],
    ) -> pd.DataFrame:
        del article, previous_extractors_output
        all_tables_info = []
        pmcid = _utils.get_pmcid_from_article_dir(article_dir)
        for table_json in _utils.get_table_info_files_from_article_dir(article_dir):
            table_info = json.loads(table_json.read_text("UTF-8"))
            table_info["pmcid"] = pmcid
            table_info["table_data_file"] = str(
                table_json.with_name(table_info["table_data_file"]).relative_to(
                    article_dir.parents[2]
                )
            )
            all_tables_info.append(table_info)
        if all_tables_info:
            return pd.DataFrame(all_tables_info)[list(self.fields)]
        return pd.DataFrame(columns=list(self.fields))
