import builtins
import importlib
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from pubget import ExitCode, _nimare


@pytest.fixture
def can_not_import_nimare(monkeypatch):
    with monkeypatch.context():
        monkeypatch.delitem(sys.modules, "nimare", False)
        monkeypatch.delitem(sys.modules, "nimare.io", False)
        _builtins_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if "nimare" in name:
                raise ImportError("nimare not installed")
            return _builtins_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", mock_import)
        yield
    importlib.reload(_nimare)


def test_nimare_import_failure(can_not_import_nimare, tmp_path):
    importlib.reload(_nimare)
    assert _nimare._NIMARE_INSTALLED is False
    output_dir, code = _nimare.make_nimare_dataset(tmp_path, tmp_path)
    assert output_dir is None
    assert code == ExitCode.ERROR


@pytest.fixture
def can_import_nimare(monkeypatch):
    monkeypatch.setitem(sys.modules, "nimare", Mock())
    monkeypatch.setitem(sys.modules, "nimare.io", Mock())
    yield
    importlib.reload(_nimare)


def test_nimare_import_success(can_import_nimare):
    importlib.reload(_nimare)
    assert _nimare._NIMARE_INSTALLED is True


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
    monkeypatch.setattr("pubget._nimare._collect_nimare_data", mock)
    _, code = _nimare.make_nimare_dataset(tmp_path, tmp_path, tmp_path)
    assert code == ExitCode.COMPLETED
    assert len(mock.mock_calls) == 0
