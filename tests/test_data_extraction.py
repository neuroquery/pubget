from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from nqdc import _download, _articles, _data_extraction


def test_extract_data_to_csv(tmp_path, entrez_mock, monkeypatch):
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path
    )
    assert code == 0
    articles_dir = Path(f"{download_dir}-articles")
    _articles.extract_articles(download_dir, articles_dir)
    data_dir = Path(f"{download_dir}-extracted_data")
    bucket = articles_dir.joinpath("000")
    bucket.mkdir(exist_ok=True)
    for i in range(20):
        bucket.joinpath(f"pmcid_745{i}.xml").write_bytes(b"")
    data_dir, code = _data_extraction.extract_data_to_csv(
        articles_dir, data_dir
    )
    assert code == 0

    # check does not repeat completed extraction
    with patch("nqdc._data_extraction._do_extract_data_to_csv") as mock:
        data_dir, code = _data_extraction.extract_data_to_csv(
            articles_dir, data_dir
        )
        assert code == 0
        assert len(mock.mock_calls) == 0

    # check extracted data
    metadata = pd.read_csv(data_dir.joinpath("metadata.csv"))
    assert metadata.shape == (7, 6)
    text = pd.read_csv(data_dir.joinpath("text.csv"))
    assert text.shape == (7, 5)
    assert text.at[0, "body"].strip() == "The text of the article"
    coordinates = pd.read_csv(data_dir.joinpath("coordinates.csv"))
    assert coordinates.shape == (14, 6)
    authors = pd.read_csv(data_dir.joinpath("authors.csv"))
    assert authors.shape == (52, 3)
    assert authors["pmcid"].nunique() == 7

    # check individual extactors are allowed to fail
    data_dir = Path(f"{download_dir}-extraction_failures-extracted_data")
    mock = Mock(side_effect=ValueError)
    monkeypatch.setattr("nqdc._authors.AuthorsExtractor.extract", mock)
    _data_extraction.extract_data_to_csv(articles_dir, data_dir)
    metadata = pd.read_csv(data_dir.joinpath("metadata.csv"))
    assert metadata.shape == (7, 6)
    authors = pd.read_csv(data_dir.joinpath("authors.csv"))
    assert authors.shape == (0, 3)

    # check returns 1 if starting from incomplete set of articles
    articles_dir.joinpath("info.json").unlink()
    data_dir.joinpath("info.json").unlink()
    data_dir, code = _data_extraction.extract_data_to_csv(
        articles_dir, data_dir
    )
    assert code == 1
