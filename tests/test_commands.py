import json
import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock
import importlib_metadata

import numpy as np
import pytest

from nqdc import _commands, _vectorization, _nimare


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
        "query-7838640309244685021f9954f8aa25fc",
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
    ("voc_source", "with_nimare", "labelbuddy_params"),
    [
        ("file", True, []),
        ("default", False, ["--labelbuddy"]),
        ("extract", False, ["--labelbuddy_part_size", "3"]),
        ("default", False, ["--labelbuddy_part_size", "-1"]),
    ],
)
def test_full_pipeline_command(
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
    test_data_dir,
    voc_source,
    with_nimare,
    labelbuddy_params,
    monkeypatch,
    mock_nimare,
):
    log_dir = tmp_path.joinpath("log")
    monkeypatch.setenv("NQDC_LOG_DIR", str(log_dir))
    args = ["run", str(tmp_path), "-q", "fMRI[abstract]", "--n_jobs", "2"]
    if with_nimare:
        args.append("--nimare")
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
    query_name = "query-7838640309244685021f9954f8aa25fc"
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


def test_steps(
    tmp_path, nq_datasets_mock, entrez_mock, test_data_dir, mock_nimare
):
    query_file = tmp_path.joinpath("query")
    query_file.write_text("fMRI[abstract]", "utf-8")
    _commands.nqdc_command(["download", str(tmp_path), "-f", str(query_file)])
    query_dir = tmp_path.joinpath("query-7838640309244685021f9954f8aa25fc")
    articlesets_dir = query_dir.joinpath("articlesets")
    assert len(list(articlesets_dir.glob("*.xml"))) == 1
    _commands.nqdc_command(["extract_articles", str(articlesets_dir)])
    articles_dir = query_dir.joinpath("articles")
    assert len(list(articles_dir.glob("**/*.xml"))) == 7
    _commands.nqdc_command(["extract_data", str(articles_dir)])
    extracted_data_dir = query_dir.joinpath("subset_allArticles_extractedData")
    assert (
        json.loads(
            extracted_data_dir.joinpath("info.json").read_text("utf-8")
        )["n_articles"]
        == 7
    )
    _commands.nqdc_command(["extract_vocabulary", str(extracted_data_dir)])
    assert query_dir.joinpath(
        "subset_allArticles_extractedVocabulary", "vocabulary.csv"
    ).is_file()
    _commands.nqdc_command(["vectorize", str(extracted_data_dir)])
    voc_checksum = _vectorization._checksum_vocabulary(
        test_data_dir.joinpath("vocabulary.csv")
    )
    vectorized_dir = query_dir.joinpath(
        f"subset_allArticles-voc_{voc_checksum}_vectorizedText"
    )
    assert (
        len(np.loadtxt(vectorized_dir.joinpath("pmcid.txt"), dtype=int)) == 7
    )
    _commands.nqdc_command(
        ["extract_labelbuddy_data", str(extracted_data_dir)]
    )
    assert query_dir.joinpath(
        "subset_allArticles_labelbuddyData", "documents_00001.jsonl"
    ).is_file()
    _commands.nqdc_command(["extract_nimare_data", str(vectorized_dir)])
    assert query_dir.joinpath(
        f"subset_allArticles-voc_{voc_checksum}_nimareDataset",
        "nimare_dataset.json",
    ).is_file()


class _PipelineStep:
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


class _StandaloneStep(_PipelineStep):
    pass


def test_plugins(
    monkeypatch,
    tmp_path,
    nq_datasets_mock,
    entrez_mock,
):

    pipeline_plugin = _PipelineStep()
    standalone_plugin = _StandaloneStep()

    def _mock_entry_point():
        return {
            "pipeline_steps": [pipeline_plugin],
            "standalone_steps": [standalone_plugin],
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
    assert standalone_plugin.arg_parser_called
    assert standalone_plugin.run_called
