from pathlib import Path
import json
from unittest.mock import patch

import pytest

from nqdc import _download, _articles, ExitCode


@pytest.mark.parametrize("n_jobs", [1, 3])
def test_extract_articles(n_jobs, tmp_path, entrez_mock):
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path
    )
    assert code == ExitCode.COMPLETED
    articles_dir = Path(f"{download_dir}-articles")
    created_dir, code = _articles.extract_articles(
        download_dir, articles_dir, n_jobs=n_jobs
    )
    assert created_dir == articles_dir
    assert code == ExitCode.COMPLETED
    assert len(list(articles_dir.glob("**/*.xml"))) == 7

    # check does not repeat completed extraction
    with patch("nqdc._articles._extract_from_articleset") as mock:
        created_dir, code = _articles.extract_articles(
            download_dir, articles_dir
        )
        assert len(mock.mock_calls) == 0
        assert code == ExitCode.COMPLETED

    # check returns 1 if download incomplete
    info_file = download_dir.joinpath("info.json")
    info = json.loads(info_file.read_text("utf-8"))
    info["is_complete"] = False
    info_file.write_text(json.dumps(info), "utf-8")
    created_dir, code = _articles.extract_articles(download_dir)
    assert created_dir == tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc", "articles"
    )
    assert code == ExitCode.INCOMPLETE
    info_file.unlink()
    _, code = _articles.extract_articles(download_dir)
    assert code == ExitCode.INCOMPLETE
