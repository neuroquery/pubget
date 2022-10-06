"""Extracting list of authors from article XML."""
import pathlib

import pandas as pd
from lxml import etree

from pubget import _utils
from pubget._typing import Extractor


class AuthorsExtractor(Extractor):
    """Extracting list of authors from article XML."""

    fields = ("pmcid", "surname", "given-names")
    name = "authors"

    def extract(
        self, article: etree.ElementTree, article_dir: pathlib.Path
    ) -> pd.DataFrame:
        authors = []
        pmcid = _utils.get_pmcid(article)
        for author_elem in article.iterfind(
            "front/article-meta/contrib-group/contrib[@contrib-type='author']"
        ):
            author_info = {"pmcid": pmcid}
            for part in [
                "name/surname",
                "name/given-names",
            ]:
                elem = author_elem.find(part)
                if elem is not None:
                    author_info[elem.tag] = elem.text
            authors.append(author_info)
        return pd.DataFrame(authors, columns=self.fields)
