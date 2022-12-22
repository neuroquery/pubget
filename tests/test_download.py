import argparse
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from pubget import ExitCode, _download


def test_download_query_results(tmp_path, entrez_mock, monkeypatch):
    entrez_mock.fail_efetch_after_n_articles = 1
    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path, retmax=3
    )
    assert code == ExitCode.ERROR
    assert download_dir == tmp_path.joinpath(
        "query_7838640309244685021f9954f8aa25fc", "articlesets"
    )
    assert download_dir.joinpath("articleset_00000.xml").is_file()
    assert not download_dir.joinpath("articleset_00001.xml").is_file()
    assert not json.loads(
        download_dir.joinpath("info.json").read_text("utf-8")
    )["is_complete"]

    entrez_mock.fail_efetch_after_n_articles = None

    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path, retmax=3, n_docs=1
    )
    assert code == ExitCode.INCOMPLETE
    assert not json.loads(
        download_dir.joinpath("info.json").read_text("utf-8")
    )["is_complete"]

    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path, retmax=3
    )
    assert code == ExitCode.COMPLETED
    assert download_dir.joinpath("articleset_00001.xml").is_file()
    assert download_dir.joinpath("articleset_00002.xml").is_file()
    assert json.loads(download_dir.joinpath("info.json").read_text("utf-8"))[
        "is_complete"
    ]
    mock = Mock()
    monkeypatch.setattr(_download, "EntrezClient", mock)
    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path, retmax=3
    )
    assert code == ExitCode.COMPLETED
    assert not mock.called


def test_download_pmcids(tmp_path, entrez_mock):
    download_dir, code = _download.download_pmcids([1, 2, 3], tmp_path)
    assert code == ExitCode.COMPLETED
    assert download_dir == tmp_path.joinpath(
        "pmcidList_55b84a9d317184fe61224bfb4a060fb0", "articlesets"
    )
    assert (
        download_dir.parent.joinpath("requested_pmcids.txt").read_text("UTF-8")
        == "1\n2\n3\n"
    )


def test_get_api_key(monkeypatch):
    monkeypatch.delenv("NCBI_API_KEY", raising=False)
    args = argparse.Namespace(api_key=None)
    key = _download._get_api_key(args)
    assert key is None
    monkeypatch.setenv("NCBI_API_KEY", "apikey")
    key = _download._get_api_key(args)
    assert key == "apikey"
    args = argparse.Namespace(api_key="apikey1")
    key = _download._get_api_key(args)
    assert key == "apikey1"


def test_get_data_dir(monkeypatch):
    monkeypatch.delenv("PUBGET_DATA_DIR", raising=False)
    args = argparse.Namespace(data_dir=None)
    with pytest.raises(RuntimeError):
        _download._get_data_dir(args)
    monkeypatch.setenv("PUBGET_DATA_DIR", "pubget_data_env")
    data_dir = _download._get_data_dir(args)
    assert data_dir == Path("pubget_data_env")
    args = argparse.Namespace(data_dir="pubget_data_args")
    data_dir = _download._get_data_dir(args)
    assert data_dir == Path("pubget_data_args")


def test_data_dir_arg(monkeypatch):
    monkeypatch.delenv("PUBGET_DATA_DIR", raising=False)
    parser = argparse.ArgumentParser()
    _download._edit_argument_parser(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(["-q", "fmri"])

    parser = argparse.ArgumentParser()
    _download._edit_argument_parser(parser)
    parser.parse_args(["pubget_data_arg", "-q", "fmri"])

    monkeypatch.setenv("PUBGET_DATA_DIR", "pubget_data_env")
    parser = argparse.ArgumentParser()
    _download._edit_argument_parser(parser)
    parser.parse_args(["-q", "fmri"])
