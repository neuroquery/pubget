import json
import argparse
from pathlib import Path
from unittest.mock import Mock

import pytest

from nqdc import _download


def test_download_articles_for_query(tmp_path, entrez_mock, monkeypatch):
    entrez_mock.fail_efetch_after_n_articles = 1
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path, retmax=3
    )
    assert code == 1
    assert download_dir == tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc", "articlesets"
    )
    assert download_dir.joinpath("articleset_00000.xml").is_file()
    assert not download_dir.joinpath("articleset_00001.xml").is_file()
    assert not json.loads(
        download_dir.joinpath("info.json").read_text("utf-8")
    )["is_complete"]

    entrez_mock.fail_efetch_after_n_articles = None
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path, retmax=3
    )
    assert code == 0
    assert download_dir.joinpath("articleset_00001.xml").is_file()
    assert download_dir.joinpath("articleset_00002.xml").is_file()
    assert json.loads(download_dir.joinpath("info.json").read_text("utf-8"))[
        "is_complete"
    ]
    mock = Mock()
    monkeypatch.setattr(_download, "EntrezClient", mock)
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path, retmax=3
    )
    assert code == 0
    assert not mock.called


def test_get_api_key(monkeypatch):
    monkeypatch.delenv("NQDC_API_KEY", raising=False)
    args = argparse.Namespace(api_key=None)
    key = _download._get_api_key(args)
    assert key is None
    monkeypatch.setenv("NQDC_API_KEY", "apikey")
    key = _download._get_api_key(args)
    assert key == "apikey"
    args = argparse.Namespace(api_key="apikey1")
    key = _download._get_api_key(args)
    assert key == "apikey1"


def test_get_data_dir(monkeypatch):
    monkeypatch.delenv("NQDC_DATA_DIR", raising=False)
    args = argparse.Namespace(data_dir=None)
    with pytest.raises(RuntimeError):
        _download._get_data_dir(args)
    monkeypatch.setenv("NQDC_DATA_DIR", "nqdc_data_env")
    data_dir = _download._get_data_dir(args)
    assert data_dir == Path("nqdc_data_env")
    args = argparse.Namespace(data_dir="nqdc_data_args")
    data_dir = _download._get_data_dir(args)
    assert data_dir == Path("nqdc_data_args")


def test_data_dir_arg(monkeypatch):
    monkeypatch.delenv("NQDC_DATA_DIR", raising=False)
    parser = argparse.ArgumentParser()
    _download._edit_argument_parser(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["-q", "fmri"])

    parser = argparse.ArgumentParser()
    _download._edit_argument_parser(parser)
    parser.parse_args(["nqdc_data_arg", "-q", "fmri"])

    monkeypatch.setenv("NQDC_DATA_DIR", "nqdc_data_env")
    parser = argparse.ArgumentParser()
    _download._edit_argument_parser(parser)
    parser.parse_args(["-q", "fmri"])
