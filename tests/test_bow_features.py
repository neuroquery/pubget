import json

import numpy as np
import pandas as pd
from scipy import sparse

from nqdc import _bow_features


def test_vectorize_corpus_to_npz(tmp_path, test_data_dir):
    _bow_features.vectorize_corpus_to_npz(
        test_data_dir.joinpath("corpus.csv"),
        test_data_dir.joinpath("vocabulary.csv"),
        tmp_path,
    )
    _check_doc_frequencies(tmp_path)
    _check_matrices(tmp_path)


def _check_matrices(data_dir):
    for source in ["title", "keywords", "abstract", "body"]:
        for kind in ["tfidf", "counts"]:
            data = sparse.load_npz(
                str(data_dir.joinpath(f"{source}_{kind}.npz"))
            )
            assert data.shape == (3, 5)
            assert data.dtype == int if kind == "counts" else float
    body_counts = sparse.load_npz(str(data_dir.joinpath("body_counts.npz"))).A
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
    op = _bow_features._voc_mapping_matrix(voc, mapping)
    assert np.allclose(op.A, [[1, 0, 0, 0], [0, 1, 1, 0], [0, 0, 0, 1]])
    assert np.allclose(op.dot(np.arange(1, len(voc) + 1)), [1, 5, 4])
