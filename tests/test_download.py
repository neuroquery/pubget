from nqdc import _download


def test_download_articles_for_query(tmp_path, entrez_mock):
    download_dir, code = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path
    )
    assert code == 0
    assert download_dir == tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc", "articlesets"
    )
    assert download_dir.joinpath("batch_00000.xml").is_file()
