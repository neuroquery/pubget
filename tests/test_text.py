from pathlib import Path
from unittest.mock import Mock

from lxml import etree

from pubget import _text


def test_text_extractor_transform_failure(monkeypatch):
    """Transforming the article to extract text is allowed to fail."""
    extractor = _text.TextExtractor()
    monkeypatch.setattr(
        etree, "XSLT", Mock(return_value=Mock(side_effect=ValueError))
    )
    assert extractor.extract(Mock(), Mock(), {}) == {}


def test_text_extractor_preserves_xref_text_by_default():
    extractor = _text.TextExtractor()
    article = etree.fromstring(
        b"""<article>
            <body>
              <p>Example ( <xref ref-type='bibr'>Doe et al.</xref> ) test.</p>
            </body>
          </article>"""
    )
    result = extractor.extract(article, Path("."), {})
    assert "Doe et al." in result["body"]
    assert "( )" not in result["body"]


def test_text_extractor_strips_xref_text_when_disabled():
    extractor = _text.TextExtractor(preserve_cross_references=False)
    article = etree.fromstring(
        b"""<article>
            <body>
              <p>Example ( <xref ref-type='bibr'>Doe et al.</xref> ) test.</p>
            </body>
          </article>"""
    )
    result = extractor.extract(article, Path("."), {})
    assert "Doe et al." not in result["body"]
    assert "( )" in result["body"]
