import json
from unittest.mock import Mock

import numpy as np
import pandas as pd
from scipy import sparse
import neuroquery
import pytest

from nqdc import _fit_neuroquery


@pytest.fixture
def extracted_and_tfidf_dir(tmp_path):
    extracted_dir = tmp_path.joinpath(
        "subset_articlesWithCoords_extractedData"
    )
    extracted_dir.mkdir()
    tfidf_dir = tmp_path.joinpath(
        "subset_articlesWithCoords-voc_myvoc_vectorizedText"
    )
    tfidf_dir.mkdir()
    rng = np.random.default_rng(0)
    pmcids = rng.integers(10000, size=30)
    metadata_cols = list("ABC")
    metadata_vals = rng.choice(
        list("abcdef"), size=(len(pmcids), len(metadata_cols))
    )
    metadata = pd.DataFrame(metadata_vals, index=pmcids, columns=metadata_cols)
    metadata.index.name = "pmcid"
    metadata.to_csv(extracted_dir.joinpath("metadata.csv"))
    coords_idx = np.concatenate([pmcids[10:], pmcids[10:], pmcids[10:]])
    coords_vals = rng.normal(0, 10, (len(coords_idx), 3))
    coords = pd.DataFrame(
        coords_vals, index=coords_idx, columns=list("xyz")
    ).sort_index()
    coords.index.name = "pmcid"
    coords.to_csv(extracted_dir.joinpath("coordinates.csv"))
    voc = [f"term {i}" for i in range(100)]
    features = voc[:50]
    mapping = {f"term {i}": f"term {i //2}" for i in range(50, 100)}
    tfidf = rng.poisson(1.0, (len(pmcids), len(features))).astype(float)
    tfidf /= tfidf.max()
    tfidf[:, ::3] = 0
    sparse.save_npz(
        str(tfidf_dir.joinpath("merged_tfidf.npz")), sparse.csr_matrix(tfidf)
    )
    voc_freq = pd.DataFrame({"term": voc, "freq": 1.0})
    voc_freq.to_csv(
        tfidf_dir.joinpath("vocabulary.csv"), index=False, header=None
    )
    feat_freq = pd.DataFrame({"term": features, "freq": 1.0})
    feat_freq.to_csv(
        tfidf_dir.joinpath("feature_names.csv"), index=False, header=None
    )
    tfidf_dir.joinpath("vocabulary.csv_voc_mapping_identity.json").write_text(
        json.dumps(mapping), "utf-8"
    )
    tfidf_dir.joinpath("info.json").write_text(
        json.dumps({"is_complete": True}), "utf-8"
    )
    extracted_dir.joinpath("info.json").write_text(
        json.dumps({"is_complete": True}), "utf-8"
    )
    return extracted_dir, tfidf_dir


@pytest.fixture
def extracted_data_dir(extracted_and_tfidf_dir):
    extracted, _ = extracted_and_tfidf_dir
    return extracted


@pytest.fixture
def tfidf_dir(extracted_and_tfidf_dir):
    _, tfidf = extracted_and_tfidf_dir
    return tfidf


def test_fit_neuroquery(extracted_data_dir, tfidf_dir):
    output_dir, code = _fit_neuroquery.fit_neuroquery(
        tfidf_dir, extracted_data_dir, n_jobs=2
    )
    assert code == 0
    model = neuroquery.NeuroQueryModel.from_data_dir(
        str(output_dir.joinpath("neuroquery_model"))
    )
    model("term 3 term 45 term 10000")
    assert output_dir.joinpath("app.py").is_file()


def test_does_not_rerun(tmp_path, monkeypatch):
    tmp_path.joinpath("info.json").write_text(
        json.dumps({"is_complete": True}),
        "utf-8",
    )
    mock = Mock()
    monkeypatch.setattr("nqdc._fit_neuroquery._NeuroQueryFit.fit", mock)
    _, code = _fit_neuroquery.fit_neuroquery(tmp_path, tmp_path, tmp_path)
    assert code == 0
    assert len(mock.mock_calls) == 0
