import json
import shutil
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from scipy import sparse

from pubget import ExitCode, _vectorization


@pytest.mark.parametrize(("with_voc", "n_jobs"), [(True, 3), (False, 1)])
def test_vectorize_corpus_to_npz(
    tmp_path, nq_datasets_mock, test_data_dir, with_voc, n_jobs
):
    input_dir = tmp_path.joinpath("extracted_data")
    shutil.copytree(test_data_dir, input_dir)
    kwargs = {}
    if with_voc:
        kwargs["vocabulary"] = input_dir.joinpath("vocabulary.csv")
    output_dir, code = _vectorization.vectorize_corpus_to_npz(
        input_dir, output_dir=tmp_path, n_jobs=n_jobs, **kwargs
    )
    assert code == ExitCode.INCOMPLETE
    input_dir.joinpath("info.json").write_text(
        json.dumps({"is_complete": True, "name": "extract_data"}), "utf-8"
    )
    output_dir, code = _vectorization.vectorize_corpus_to_npz(
        input_dir, output_dir=tmp_path, n_jobs=n_jobs, **kwargs
    )
    assert code == ExitCode.COMPLETED
    _check_pmcids(tmp_path)
    _check_doc_frequencies(tmp_path)
    _check_matrices(tmp_path)
    with patch("pubget._vectorization._do_vectorize_corpus_to_npz") as mock:
        output_dir, code = _vectorization.vectorize_corpus_to_npz(
            input_dir, output_dir=tmp_path, n_jobs=n_jobs, **kwargs
        )
        assert code == ExitCode.COMPLETED
        assert len(mock.mock_calls) == 0


def _check_pmcids(data_dir):
    pmcids = np.loadtxt(data_dir.joinpath("pmcid.txt"), dtype=int)
    assert set(pmcids) == {123, 456, 789}


def _check_matrices(data_dir):
    for source in ["title", "keywords", "abstract", "body"]:
        for kind in ["tfidf", "counts"]:
            data = sparse.load_npz(
                str(data_dir.joinpath(f"{source}_{kind}.npz"))
            )
            assert data.shape == (3, 5)
            assert data.dtype == int if kind == "counts" else float
    body_counts = sparse.load_npz(
        str(data_dir.joinpath("body_counts.npz"))
    ).toarray()
    assert (
        body_counts
        == [
            [0, 2, 1, 0, 0],
            [1, 1, 0, 0, 0],
            [0, 0, 0, 0, 0],
        ]
    ).all()


def _check_doc_frequencies(data_dir):
    features_freq = pd.read_csv(
        data_dir.joinpath("feature_names.csv"), index_col=0, header=None
    ).iloc[:, 0]
    assert (
        features_freq
        == pd.Series(
            [0.5, 0.75, 0.5, 0.25, 0.25],
            index=[
                "auditory cortex",
                "brain",
                "memory",
                "motor",
                "visual memory",
            ],
        )
    ).all()
    all_freq = pd.read_csv(
        data_dir.joinpath("vocabulary.csv"), index_col=0, header=None
    ).iloc[:, 0]
    assert (
        all_freq
        == pd.Series(
            [0.5, 0.5, 0.75, 0.5, 0.25, 0.25],
            index=[
                "auditory cortex",
                "brain",
                "brains",
                "memory",
                "motor",
                "visual memory",
            ],
        )
    ).all()
    assert json.loads(
        data_dir.joinpath(
            "vocabulary.csv_voc_mapping_identity.json"
        ).read_text(encoding="utf-8")
    ) == {"brains": "brain"}


def test_voc_mapping_matrix():
    voc = ["amygdala", "brain stem", "brainstem", "cortex"]
    mapping = {"brain stem": "brainstem"}
    op = _vectorization._voc_mapping_matrix(voc, mapping)
    assert np.allclose(
        op.toarray(), [[1, 0, 0, 0], [0, 1, 1, 0], [0, 0, 0, 1]]
    )
    assert np.allclose(op.dot(np.arange(1, len(voc) + 1)), [1, 5, 4])


def test_load_voc_mapping(tmp_path):
    voc_file = tmp_path.joinpath("vocabulary.txt")
    assert _vectorization._load_voc_mapping(voc_file) == {}
    mapping = {"brains": "brain"}
    voc_file.with_name(
        f"{voc_file.name}_voc_mapping_identity.json"
    ).write_text(json.dumps(mapping), "utf-8")
    assert _vectorization._load_voc_mapping(voc_file) == mapping
