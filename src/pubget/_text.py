"""Extracting text from XML articles."""
import logging
import pathlib
from typing import Dict, Union

from lxml import etree

from pubget import _utils
from pubget._typing import Extractor

_LOG = logging.getLogger(__name__)


class TextExtractor(Extractor):
    """Extracting text from XML articles."""

    fields = ("pmcid", "title", "keywords", "abstract", "body")
    name = "text"

    def extract(
        self, article: etree.ElementTree, article_dir: pathlib.Path
    ) -> Dict[str, Union[str, int]]:
        result: Dict[str, Union[str, int]] = {}
        # Stylesheet is not parsed in init because lxml.XSLT cannot be pickled
        # so that would prevent the extractor from being passed to
        # multiprocessing map. Parsing is cached.
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
        result["pmcid"] = int(result["pmcid"])
        return result
