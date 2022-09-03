import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import importlib_metadata
import numpy as np
import pandas as pd
import pytest

from nqdc import _commands, _nimare, _vectorization


def _patch_neuroquery(monkeypatch):
    monkeypatch.setattr(
        "nqdc._model_data.ModelData._MIN_DOCUMENT_FREQUENCY", 1
    )
    monkeypatch.setattr("nqdc._fit_neuroquery.SmoothedRegression", MagicMock())
    monkeypatch.setattr("nqdc._fit_neuroquery.normalize", MagicMock())


def test_full_pipeline_command_with_nimare(
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
    test_data_dir,
    monkeypatch,
):
    pytest.importorskip("nimare")
    args = ["run", str(tmp_path), "-q", "fMRI[abstract]", "--nimare"]
    code = _commands.nqdc_command(args)
    assert code == 0
    voc_file = test_data_dir.joinpath("vocabulary.csv")
    voc_checksum = _vectorization._checksum_vocabulary(voc_file)
    nimare_file = tmp_path.joinpath(
        "query_7838640309244685021f9954f8aa25fc",
        f"subset_allArticles-voc_{voc_checksum}_nimareDataset",
        "nimare_dataset.json",
    )
    assert nimare_file.is_file()
    nimare_data = json.loads(nimare_file.read_text("utf-8"))
    assert "contrasts" in list(nimare_data.values())[0]


@pytest.fixture
def mock_nimare(monkeypatch):
    nimare = Mock()
    nimare_io = Mock()
    nimare.io = nimare_io
    monkeypatch.setitem(sys.modules, "nimare", nimare)
    monkeypatch.setitem(sys.modules, "nimare.io", nimare_io)
    monkeypatch.setattr(_nimare, "_NIMARE_INSTALLED", True)
    monkeypatch.setattr(_nimare, "nimare", nimare, raising=False)
    # monkeypatch.setattr(_nimare.nimare, "io", nimare_io)

    def convert(coords, meta, output_file, **kwargs):
        Path(output_file).write_text(json.dumps("nimare"), "utf-8")

    nimare_io.convert_neurosynth_to_json.side_effect = convert
    return nimare_io.convert_neurosynth_to_json


@pytest.mark.parametrize(
    (
        "voc_source",
        "with_nimare",
        "labelbuddy_params",
        "with_fit_neuroquery",
        "with_fit_neurosynth",
    ),
    [
        ("file", True, [], False, True),
        ("default", False, ["--labelbuddy"], False, False),
        ("extract", False, ["--labelbuddy_part_size", "3"], False, False),
        ("default", False, ["--labelbuddy_part_size", "-1"], True, False),
    ],
)
def test_full_pipeline_command(
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
    test_data_dir,
    voc_source,
    with_nimare,
    with_fit_neuroquery,
    with_fit_neurosynth,
    labelbuddy_params,
    monkeypatch,
    mock_nimare,
):
    log_dir = tmp_path.joinpath("log")
    _patch_neuroquery(monkeypatch)
    monkeypatch.setenv("NQDC_LOG_DIR", str(log_dir))
    args = ["run", str(tmp_path), "-q", "fMRI[abstract]", "--n_jobs", "2"]
    if with_nimare:
        args.append("--nimare")
    if with_fit_neuroquery:
        args.append("--fit_neuroquery")
    if with_fit_neurosynth:
        args.append("--fit_neurosynth")
    args.extend(labelbuddy_params)
    voc_file = test_data_dir.joinpath("vocabulary.csv")
    if voc_source == "file":
        args.extend(["-v", str(voc_file)])
    elif voc_source == "extract":
        args.extend(["--extract_vocabulary"])
    else:
        assert voc_source == "default"
    code = _commands.nqdc_command(args)
    assert code == 0
    query_name = "query_7838640309244685021f9954f8aa25fc"
    if voc_source == "extract":
        voc_file = tmp_path.joinpath(
            query_name,
            "subset_allArticles_extractedVocabulary",
            "vocabulary.csv",
        )
        assert voc_file.is_file()
    voc_checksum = _vectorization._checksum_vocabulary(voc_file)
    assert tmp_path.joinpath(
        query_name,
        f"subset_allArticles-voc_{voc_checksum}_vectorizedText",
        "pmcid.txt",
    ).is_file()
    if with_fit_neuroquery:
        assert tmp_path.joinpath(
            query_name,
            f"subset_allArticles-voc_{voc_checksum}_neuroqueryModel",
        ).is_dir()
    if with_fit_neurosynth:
        assert tmp_path.joinpath(
            query_name,
            f"subset_allArticles-voc_{voc_checksum}_neurosynthResults",
        ).is_dir()
    if with_nimare:
        assert tmp_path.joinpath(
            query_name,
            f"subset_allArticles-voc_{voc_checksum}_nimareDataset",
        ).is_dir()
        mock_nimare.assert_called_once()
    if labelbuddy_params:
        with open(
            tmp_path.joinpath(
                query_name,
                "subset_allArticles_labelbuddyData",
                "documents_00001.jsonl",
            )
        ) as f:
            assert len(list(f)) == (3 if "3" in labelbuddy_params else 7)
    assert len(list(log_dir.glob("*"))) == 1


def _check_download_query_file_output(tmp_path):
    query_file = tmp_path.joinpath("query")
    query_file.write_text("fMRI[abstract]", "utf-8")
    _commands.nqdc_command(["download", str(tmp_path), "-f", str(query_file)])
    query_dir = tmp_path.joinpath("query_7838640309244685021f9954f8aa25fc")
    articlesets_dir = query_dir.joinpath("articlesets")
    assert len(list(articlesets_dir.glob("*.xml"))) == 1
    return articlesets_dir


def _check_download_pmcid_list_output(tmp_path):
    pmcids_file = tmp_path.joinpath("pmcids")
    pmcids_file.write_text("\n".join(map(str, range(7))), "utf-8")
    _commands.nqdc_command(
        ["download", str(tmp_path), "--pmcids_file", str(pmcids_file)]
    )
    query_dir = tmp_path.joinpath("pmcidList_94281be5de7c35a42f8abaf41d934ace")
    articlesets_dir = query_dir.joinpath("articlesets")
    assert len(list(articlesets_dir.glob("*.xml"))) == 1
    return articlesets_dir


def _check_extract_articles_output(articlesets_dir):
    _commands.nqdc_command(["extract_articles", str(articlesets_dir)])
    articles_dir = articlesets_dir.parent.joinpath("articles")
    assert len(list(articles_dir.glob("**/article.xml"))) == 7
    return articles_dir


def _check_extract_data_output(articles_dir):
    _commands.nqdc_command(["extract_data", str(articles_dir)])
    extracted_data_dir = articles_dir.parent.joinpath(
        "subset_allArticles_extractedData"
    )
    assert (
        json.loads(
            extracted_data_dir.joinpath("info.json").read_text("utf-8")
        )["n_articles"]
        == 7
    )
    return extracted_data_dir


def _check_extract_vocabulary_output(extracted_data_dir, articles_dir):
    extracted_data_dir = _check_extract_data_output(articles_dir)
    _commands.nqdc_command(["extract_vocabulary", str(extracted_data_dir)])
    voc_dir = extracted_data_dir.parent.joinpath(
        "subset_allArticles_extractedVocabulary"
    )
    assert voc_dir.joinpath("vocabulary.csv").is_file()
    return voc_dir


def _check_vectorize_output(extracted_data_dir, test_data_dir):
    _commands.nqdc_command(["vectorize", str(extracted_data_dir)])
    voc_checksum = _vectorization._checksum_vocabulary(
        test_data_dir.joinpath("vocabulary.csv")
    )
    vectorized_dir = extracted_data_dir.parent.joinpath(
        f"subset_allArticles-voc_{voc_checksum}_vectorizedText"
    )
    assert (
        len(np.loadtxt(vectorized_dir.joinpath("pmcid.txt"), dtype=int)) == 7
    )
    return vectorized_dir, voc_checksum


def _check_fit_neuroquery_output(vectorized_dir, voc_checksum, monkeypatch):
    _patch_neuroquery(monkeypatch)
    _commands.nqdc_command(["fit_neuroquery", str(vectorized_dir)])
    nq_dir = vectorized_dir.parent.joinpath(
        f"subset_allArticles-voc_{voc_checksum}_neuroqueryModel",
        "neuroquery_model",
    )
    voc = pd.read_csv(nq_dir.joinpath("vocabulary.csv"))
    assert len(voc) == 3
    return nq_dir


def _check_fit_neurosynth_output(vectorized_dir, voc_checksum, monkeypatch):
    monkeypatch.setattr(
        "nqdc._model_data.ModelData._MIN_DOCUMENT_FREQUENCY", 1
    )
    _commands.nqdc_command(["fit_neurosynth", str(vectorized_dir)])
    ns_dir = vectorized_dir.parent.joinpath(
        f"subset_allArticles-voc_{voc_checksum}_neurosynthResults",
        "neurosynth_maps",
    )
    assert list(ns_dir.glob("*.nii.gz"))
    return ns_dir


def _check_extract_labelbuddy_data_output(extracted_data_dir):
    _commands.nqdc_command(
        ["extract_labelbuddy_data", str(extracted_data_dir)]
    )
    labelbuddy_dir = extracted_data_dir.parent.joinpath(
        "subset_allArticles_labelbuddyData"
    )
    assert labelbuddy_dir.joinpath("documents_00001.jsonl").is_file()
    return labelbuddy_dir


def _check_extract_nimare_data_output(vectorized_dir, voc_checksum):
    _commands.nqdc_command(["extract_nimare_data", str(vectorized_dir)])
    nimare_dir = vectorized_dir.parent.joinpath(
        f"subset_allArticles-voc_{voc_checksum}_nimareDataset",
    )
    assert nimare_dir.joinpath("nimare_dataset.json").is_file()
    return nimare_dir


@pytest.mark.parametrize("download_mode", ["query", "pmcid"])
def test_commands(
    download_mode,
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
    test_data_dir,
    mock_nimare,
    monkeypatch,
):
    if download_mode == "query":
        articlesets_dir = _check_download_query_file_output(tmp_path)
    else:
        assert download_mode == "pmcid"
        articlesets_dir = _check_download_pmcid_list_output(tmp_path)
    articles_dir = _check_extract_articles_output(articlesets_dir)
    extracted_data_dir = _check_extract_data_output(articles_dir)
    _check_extract_vocabulary_output(extracted_data_dir, articles_dir)
    vectorized_dir, voc_checksum = _check_vectorize_output(
        extracted_data_dir, test_data_dir
    )
    _check_fit_neuroquery_output(vectorized_dir, voc_checksum, monkeypatch)
    _check_fit_neurosynth_output(vectorized_dir, voc_checksum, monkeypatch)
    _check_extract_labelbuddy_data_output(extracted_data_dir)
    _check_extract_nimare_data_output(vectorized_dir, voc_checksum)


class _PluginPipelineStep:
    name = "myplugin"
    short_description = "myplugin"

    def __init__(self):
        self.arg_parser_called = False
        self.run_called = False

    def edit_argument_parser(self, argument_parser):
        self.arg_parser_called = True

    def run(self, args, previous_steps_output):
        self.run_called = True
        return "", 0


class _PluginCommand:
    name = "myplugin"
    short_description = "myplugin"

    def __init__(self):
        self.arg_parser_called = False
        self.run_called = False

    def edit_argument_parser(self, argument_parser):
        self.arg_parser_called = True

    def run(self, args):
        self.run_called = True
        return 0


def test_plugins(
    monkeypatch,
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
):

    pipeline_plugin = _PluginPipelineStep()
    command_plugin = _PluginCommand()

    def _mock_entry_point():
        return {
            "pipeline_steps": [pipeline_plugin],
            "commands": [command_plugin],
        }

    ep = Mock()
    ep.load.return_value = _mock_entry_point
    all_ep = Mock()
    all_ep.select.return_value = [ep]
    metadata_ep = MagicMock()
    metadata_ep.return_value = all_ep
    monkeypatch.setattr(importlib_metadata, "entry_points", metadata_ep)
    args = ["run", str(tmp_path), "-q", "fMRI[abstract]"]
    _commands.nqdc_command(args)
    assert pipeline_plugin.arg_parser_called
    assert pipeline_plugin.run_called
    _commands.nqdc_command(["myplugin"])
    assert command_plugin.arg_parser_called
    assert command_plugin.run_called
