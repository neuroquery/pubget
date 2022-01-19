from pathlib import Path

from nqdc import _download, _articles


def test_download_articles_for_search_term(tmp_path, entrez_mock):
    download_dir = _download.download_articles_for_search_term(
        "fMRI[abstract]", tmp_path
    )
    articles_dir = Path(f"{download_dir}-articles")
    _articles.extract_articles(download_dir, articles_dir)
    assert len(list(articles_dir.glob("**/*.xml"))) == 7
