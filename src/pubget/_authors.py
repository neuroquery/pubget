"""Extracting list of authors from article XML."""
import pathlib
from typing import Dict

import pandas as pd
from lxml import etree

from pubget import _utils
from pubget._typing import Extractor, Records


class AuthorsExtractor(Extractor):
    """Extracting list of authors from article XML."""

    fields = ("id", "firstname", "lastname")
    name = "authors"

    def extract(
        self,
        article: etree.ElementTree,
        article_dir: pathlib.Path,
        previous_extractors_output: Dict[str, Records],
    ) -> pd.DataFrame:
        del article_dir, previous_extractors_output
        authors = []
        id = _utils.get_id(article)
        if "pmcid" in id:
            author_indicator = "front/article-meta/contrib-group/contrib[@contrib-type='author']"
            firstname_indicator = "name/given-names"
            lastname_indicator = "name/surname"
        elif "pmid" in id:
            author_indicator = ".//Author"
            firstname_indicator = "ForeName"
            lastname_indicator = "LastName"
        firstname_field = "firstname"
        lastname_field = "lastname"

        for author_elem in article.iterfind(author_indicator):
            author_info = {"id": id}
            for part, field in zip(
                [firstname_indicator, lastname_indicator],
                [firstname_field, lastname_field],
            ):
                elem = author_elem.find(part)

                if elem is not None:
                    author_info[field] = elem.text
            authors.append(author_info)
        return pd.DataFrame(authors, columns=self.fields)
