from pathlib import Path
from unittest.mock import Mock, patch
import json

import pytest

import numpy as np
import pandas as pd

from nqdc import _download, _articles, _data_extraction


@pytest.fixture
def articles_dir(tmp_path, entrez_mock):
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path
    )
    assert code == 0
    articles_dir = Path(f"{download_dir}-articles")
    _articles.extract_articles(download_dir, articles_dir)
    bucket = articles_dir.joinpath("000")
    bucket.mkdir(exist_ok=True)
    for i in range(150):
        bucket.joinpath(f"pmcid_745{i}.xml").write_bytes(b"")
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
    assert code == 0

    # check does not repeat completed extraction
    with patch("nqdc._data_extraction._do_extract_data_to_csv") as mock:
        data_dir, code = _data_extraction.extract_data_to_csv(
            articles_dir,
            data_dir,
            articles_with_coords_only=articles_with_coords_only,
        )
        assert code == 0
        assert len(mock.mock_calls) == 0

    _check_extracted_data(data_dir, articles_with_coords_only)


def test_extractor_failures(articles_dir, tmp_path, monkeypatch):
    data_dir = Path(f"{tmp_path}-extraction_failures-extracted_data")
    mock = Mock(side_effect=ValueError)
    monkeypatch.setattr("nqdc._authors.AuthorsExtractor.extract", mock)
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
    assert code == 1


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
