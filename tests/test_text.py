from unittest.mock import Mock

from nqdc import _text


def test_text_extractor_transform_failure(monkeypatch):
    extractor = _text.TextExtractor()
    monkeypatch.setattr(extractor, "_stylesheet", Mock(side_effect=ValueError))
    assert extractor.extract(Mock()) == {}
