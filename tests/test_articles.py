from pathlib import Path
import json

from nqdc import _download, _articles


def test_extract_articles(tmp_path, entrez_mock):
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path
    )
    assert code == 0
    articles_dir = Path(f"{download_dir}-articles")
    created_dir, code = _articles.extract_articles(download_dir, articles_dir)
    assert created_dir == articles_dir
    assert code == 0
    assert len(list(articles_dir.glob("**/*.xml"))) == 7
    info_file = download_dir.joinpath("info.json")
    info = json.loads(info_file.read_text("utf-8"))
    info["download_complete"] = False
    info_file.write_text(json.dumps(info), "utf-8")
    created_dir, code = _articles.extract_articles(download_dir)
    assert created_dir == tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc", "articles"
    )
    assert code == 1
    info_file.unlink()
    _, code = _articles.extract_articles(download_dir)
    assert code == 1
    info["download_complete"] = True
    info_file.write_text(json.dumps(info), "utf-8")
