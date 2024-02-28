"""Extracting text from XML articles."""
import logging
import pathlib
from typing import Dict, Union

from lxml import etree

from pubget import _utils
from pubget._typing import Extractor, Records

_LOG = logging.getLogger(__name__)


class TextExtractor(Extractor):
    """Extracting text from XML articles."""

    fields = ("id", "title", "keywords", "abstract", "body")
    name = "text"

    def extract(
        self,
        article: etree.ElementTree,
        article_dir: pathlib.Path,
        previous_extractors_output: Dict[str, Records],
    ) -> Dict[str, Union[str, int]]:
        del article_dir, previous_extractors_output
        result: Dict[str, Union[str, int]] = {}
        # Stylesheet is not parsed in init because lxml.XSLT cannot be pickled
        # so that would prevent the extractor from being passed to
        # multiprocessing map. Parsing is cached.
        id = _utils.get_id(article)
        if "pmcid" in id:
            stylesheet = _utils.load_stylesheet("text_extraction.xsl")
            try:
                transformed = stylesheet(article)
            except Exception:
                _LOG.exception(
                    f"failed to transform article: {stylesheet.error_log}"
                )
                return result
            for part_name in self.fields:
                elem = transformed.find(part_name)
                result[part_name] = elem.text
            result["id"] = id

        elif "pmid" in id:
            result["id"] = id
            result["title"] = article.find(".//ArticleTitle").text
            keywords = []
            for item in article.iterfind(".//KeywordList/Keyword"):
                keywords.append(item.text)
            keywords = "\n".join(keywords)
            result["keywords"] = keywords
            abstract_sections = article.xpath(
                "//Article/Abstract/AbstractText"
            )
            abstract = ""
            for section in abstract_sections:
                try:
                    abstract = abstract + section.text + " "
                except TypeError:
                    continue
            result["abstract"] = abstract
            result["body"] = ""
        return result
