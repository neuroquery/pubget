import builtins
from pathlib import Path
import json
import sys
from unittest.mock import Mock

import pytest

from nqdc import _nimare


def test_nimare_import_failure(monkeypatch, tmp_path):
    monkeypatch.delitem(sys.modules, "nimare", False)
    monkeypatch.delitem(sys.modules, "nimare.io", False)
    _builtins_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if "nimare" in name:
            raise ImportError("nimare not installed")
        return _builtins_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", mock_import)
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


def test_does_not_rerun(tmp_path, monkeypatch):
    tmp_path.joinpath("info.json").write_text(
        json.dumps({"is_complete": True, "name": "nimare_dataset_creation"}),
        "utf-8",
    )
    mock = Mock()
    monkeypatch.setattr("nqdc._nimare._collect_nimare_data", mock)
    _, code = _nimare.make_nimare_dataset(tmp_path, tmp_path, tmp_path)
    assert code == 0
    assert len(mock.mock_calls) == 0
