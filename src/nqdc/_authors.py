"""Extracting list of authors from article XML."""
import pandas as pd
from lxml import etree

from nqdc._typing import BaseExtractor
from nqdc import _utils


class AuthorsExtractor(BaseExtractor):
    """Extracting list of authors from article XML."""

    fields = ("pmcid", "surname", "given-names")
    name = "authors"

    def extract(self, article: etree.ElementTree) -> pd.DataFrame:
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
