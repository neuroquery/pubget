import argparse
import json
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from pubget import (
    ExitCode,
    _articles,
    _data_extraction,
    _download,
    _typing,
    _utils,
)


@pytest.fixture
def articles_dir(tmp_path, entrez_mock):
    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path
    )
    assert code == ExitCode.COMPLETED
    articles_dir = Path(f"{download_dir}-articles")
    _articles.extract_articles(download_dir, articles_dir)
    bucket = articles_dir.joinpath("000")
    bucket.mkdir(exist_ok=True)
    for i in range(150):
        bucket.joinpath(f"pmcid_745{i}.xml").write_bytes(b"")
    return articles_dir


@pytest.fixture
def empty_articles_dir(tmp_path, entrez_mock):
    entrez_mock.fail_efetch_after_n_articles = 0
    download_dir, code = _download.download_query_results(
        "fMRI[abstract]", tmp_path
    )
    articles_dir = Path(f"{download_dir}-articles")
    _articles.extract_articles(download_dir, articles_dir)
    return articles_dir


def _check_extracted_data(data_dir, articles_with_coords_only):
    n_articles = 6 if articles_with_coords_only else 7
    n_authors = 33 if articles_with_coords_only else 36
    metadata = pd.read_csv(data_dir.joinpath("metadata.csv"))
    assert metadata.shape == (n_articles, 7)
    space = pd.read_csv(data_dir.joinpath("coordinate_space.csv"))
    assert space.shape == (n_articles, 2)
    text = pd.read_csv(data_dir.joinpath("text.csv"))
    assert text.shape == (n_articles, 5)
    assert text.at[0, "body"].strip().startswith("The text")
    coordinates = pd.read_csv(data_dir.joinpath("coordinates.csv"))
    assert coordinates.shape == (12, 6)
    authors = pd.read_csv(data_dir.joinpath("authors.csv"))
    assert authors.shape == (n_authors, 3)
    assert authors["pmcid"].nunique() == n_articles
    n_links = 7 if articles_with_coords_only else 10
    links = pd.read_csv(data_dir.joinpath("links.csv"))
    assert links.shape == (n_links, 3)
    info = json.loads(data_dir.joinpath("info.json").read_text("utf-8"))
    assert info["is_complete"] is True
    assert info["n_articles"] == n_articles


@pytest.mark.parametrize(
    ("articles_with_coords_only", "n_jobs"), [(True, 1), (False, 3)]
)
def test_extract_data_to_csv(
    tmp_path,
    articles_dir,
    entrez_mock,
    monkeypatch,
    articles_with_coords_only,
    n_jobs,
):
    data_dir = tmp_path.joinpath("extracted_data")
    data_dir, code = _data_extraction.extract_data_to_csv(
        articles_dir,
        data_dir,
        articles_with_coords_only=articles_with_coords_only,
        n_jobs=n_jobs,
    )
    assert code == ExitCode.COMPLETED

    # check does not repeat completed extraction
    with patch("pubget._data_extraction._do_extract_data_to_csv") as mock:
        data_dir, code = _data_extraction.extract_data_to_csv(
            articles_dir,
            data_dir,
            articles_with_coords_only=articles_with_coords_only,
        )
        assert code == ExitCode.COMPLETED
        assert len(mock.mock_calls) == 0

    _check_extracted_data(data_dir, articles_with_coords_only)


def test_extractor_failures(articles_dir, tmp_path, monkeypatch):
    data_dir = Path(f"{tmp_path}-extraction_failures-extracted_data")
    mock = Mock(side_effect=ValueError)
    monkeypatch.setattr("pubget._authors.AuthorsExtractor.extract", mock)
    _data_extraction.extract_data_to_csv(articles_dir, data_dir)

    metadata = pd.read_csv(data_dir.joinpath("metadata.csv"))
    assert metadata.shape == (7, 7)
    authors = pd.read_csv(data_dir.joinpath("authors.csv"))
    assert authors.shape == (0, 3)


def test_extract_from_incomplete_articles(articles_dir, tmp_path):
    articles_dir.joinpath("info.json").unlink()
    data_dir, code = _data_extraction.extract_data_to_csv(
        articles_dir, tmp_path.joinpath("extracted_data")
    )
    assert code == ExitCode.INCOMPLETE


@pytest.mark.parametrize(
    ("data", "with_coords", "expected"),
    [
        (None, False, False),
        ({"coordinates": np.ones(3)}, True, True),
        ({}, False, True),
        ({}, True, False),
        ({"coordinates": np.array([])}, True, False),
    ],
)
def test_should_write(data, with_coords, expected):
    assert _data_extraction._should_write(data, with_coords) == expected


def test_stop_pipeline(empty_articles_dir):
    step = _data_extraction.DataExtractionStep()
    args = argparse.Namespace(articles_with_coords_only=False, n_jobs=1)
    previous_steps = {"extract_articles": empty_articles_dir}
    with pytest.raises(_typing.StopPipeline, match=r"No articles.*"):
        step.run(args, previous_steps)


def test_config_worker_logging():
    _data_extraction._config_worker_logging()
    _utils.configure_logging()
