import logging

from nqdc import _utils

_LOG = logging.getLogger(__name__)


class TextExtractor:
    fields = ("pmcid", "title", "keywords", "abstract", "body")

    def __init__(self):
        self._stylesheet = _utils.load_stylesheet("text_extraction.xsl")

    def __call__(self, article):
        return self._extract_text_from_article(article, self._stylesheet)

    def _extract_text_from_article(self, article, stylesheet):
        result = {}
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
