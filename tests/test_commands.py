import json
import sys
from unittest.mock import Mock

import numpy as np
import pytest

from nqdc import _commands, _vectorization


def test_full_pipeline_command_with_nimare(
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
    test_data_dir,
    monkeypatch,
):
    pytest.importorskip("nimare")
    args = [str(tmp_path), "-q", "fMRI[abstract]", "--nimare"]
    code = _commands.full_pipeline_command(args)
    assert code == 0
    voc_file = test_data_dir.joinpath("vocabulary.csv")
    voc_checksum = _vectorization._checksum_vocabulary(voc_file)
    assert tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc",
        f"subset_allArticles-voc_{voc_checksum}_nimareDataset",
        "nimare_dataset.json",
    ).is_file()


@pytest.mark.parametrize(
    ("with_voc", "with_nimare", "labelbuddy_params"),
    [
        (True, True, []),
        (False, False, ["--labelbuddy"]),
        (False, False, ["--labelbuddy_batch_size", "3"]),
    ],
)
def test_full_pipeline_command(
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
    test_data_dir,
    with_voc,
    with_nimare,
    labelbuddy_params,
    monkeypatch,
):
    monkeypatch.setitem(sys.modules, "nimare", Mock())
    nimare_io = Mock()
    monkeypatch.setitem(sys.modules, "nimare.io", nimare_io)
    log_dir = tmp_path.joinpath("log")
    monkeypatch.setenv("NQDC_LOG_DIR", str(log_dir))
    args = [str(tmp_path), "-q", "fMRI[abstract]", "--n_jobs", "2"]
    if with_nimare:
        args.append("--nimare")
    args.extend(labelbuddy_params)
    voc_file = test_data_dir.joinpath("vocabulary.csv")
    if with_voc:
        args.extend(["-v", str(voc_file)])
    code = _commands.full_pipeline_command(args)
    assert code == 0
    voc_checksum = _vectorization._checksum_vocabulary(voc_file)
    assert tmp_path.joinpath(
        "query-7838640309244685021f9954f8aa25fc",
        f"subset_allArticles-voc_{voc_checksum}_vectorizedText",
        "pmcid.txt",
    ).is_file()
    if with_nimare:
        assert tmp_path.joinpath(
            "query-7838640309244685021f9954f8aa25fc",
            f"subset_allArticles-voc_{voc_checksum}_nimareDataset",
        ).is_dir()
        nimare_io.convert_neurosynth_to_json.assert_called_once()
    if labelbuddy_params:
        assert tmp_path.joinpath(
            "query-7838640309244685021f9954f8aa25fc",
            "subset_allArticles_labelbuddyData",
            "documents_00000.jsonl",
        ).is_file()
    assert len(list(log_dir.glob("*"))) == 1


def test_steps(tmp_path, nq_datasets_mock, entrez_mock, test_data_dir):
    query_file = tmp_path.joinpath("query")
    query_file.write_text("fMRI[abstract]", "utf-8")
    _commands.download_command([str(tmp_path), "-f", str(query_file)])
    query_dir = tmp_path.joinpath("query-7838640309244685021f9954f8aa25fc")
    articlesets_dir = query_dir.joinpath("articlesets")
    assert len(list(articlesets_dir.glob("*.xml"))) == 1
    _commands.extract_articles_command([str(articlesets_dir)])
    articles_dir = query_dir.joinpath("articles")
    assert len(list(articles_dir.glob("**/*.xml"))) == 7
    _commands.extract_data_command([str(articles_dir)])
    extracted_data_dir = query_dir.joinpath("subset_allArticles_extractedData")
    assert (
        json.loads(
            extracted_data_dir.joinpath("info.json").read_text("utf-8")
        )["n_articles"]
        == 7
    )
    _commands.vectorize_command([str(extracted_data_dir)])
    voc_checksum = _vectorization._checksum_vocabulary(
        test_data_dir.joinpath("vocabulary.csv")
    )
    vectorized_dir = query_dir.joinpath(
        "subset_allArticles-" f"voc_{voc_checksum}_vectorizedText"
    )
    assert (
        len(np.loadtxt(vectorized_dir.joinpath("pmcid.txt"), dtype=int)) == 7
    )
