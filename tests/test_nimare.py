from pathlib import Path
import sys
from unittest.mock import Mock

import pytest

from nqdc import _nimare


def test_nimare_import_failure(monkeypatch, tmp_path):
    monkeypatch.delitem(sys.modules, "nimare", False)
    monkeypatch.delitem(sys.modules, "nimare.io", False)
    monkeypatch.setattr("builtins.__import__", Mock(side_effect=ImportError))
    output_dir, code = _nimare.make_nimare_dataset(tmp_path, tmp_path)
    assert output_dir is None
    assert code == 1


@pytest.mark.parametrize(
    ("dir_name", "expected"),
    [
        ("something_something-voc_myvoc_vectorizedText", "myvoc"),
        ("something-voc_-not", "UNKNOWN"),
        ("", "UNKNOWN"),
    ],
)
def test_get_vocabulary_name(dir_name, expected):
    assert _nimare._get_vocabulary_name(Path(dir_name)) == expected
