from pathlib import Path

import pandas as pd

from nqdc import _download, _articles, _data_extraction


def test_extract_to_csv(tmp_path, entrez_mock):
    download_dir = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path
    )
    articles_dir = Path(f"{download_dir}-articles")
    _articles.extract_articles(download_dir, articles_dir)
    data_dir = Path(f"{download_dir}-extracted_data")
    _data_extraction.extract_to_csv(articles_dir, data_dir)
    metadata = pd.read_csv(data_dir / "metadata.csv")
    assert metadata.shape == (7, 3)
    text = pd.read_csv(data_dir / "text.csv")
    assert text.shape == (7, 5)
    assert text.at[0, "body"].strip() == "The text of the article"
    coordinates = pd.read_csv(data_dir / "coordinates.csv")
    assert coordinates.shape == (14, 6)
