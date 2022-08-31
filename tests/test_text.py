from unittest.mock import Mock

from lxml import etree

from nqdc import _text


def test_text_extractor_transform_failure(monkeypatch):
    """Transforming the article to extract text is allowed to fail."""
    extractor = _text.TextExtractor()
    monkeypatch.setattr(
        etree, "XSLT", Mock(return_value=Mock(side_effect=ValueError))
    )
    assert extractor.extract(Mock(), Mock()) == {}
