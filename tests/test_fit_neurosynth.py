import json
from unittest.mock import Mock

import numpy as np
from scipy import stats, sparse

from nqdc import _fit_neurosynth, ExitCode


def test_chi_square():
    rng = np.random.default_rng(0)
    n_studies, n_voxels = 30, 40
    brain_maps = rng.integers(2, size=(n_studies, n_voxels)).astype(bool)
    assert brain_maps.sum()
    term_vec_sp = sparse.csc_matrix(
        rng.integers(2, size=n_studies, dtype="int32")[:, None]
    )
    assert term_vec_sp.sum()
    z_vals = _fit_neurosynth._chi_square(
        brain_maps, brain_maps.sum(axis=0), term_vec_sp
    )
    term_vec = term_vec_sp.A.ravel().astype(bool)
    stats_z_vals = []
    normal = stats.norm()
    for voxel in range(n_voxels):
        activations = brain_maps[:, voxel]
        contingency = np.empty((2, 2), dtype="int32")
        contingency[0, 0] = ((~activations) & (~term_vec)).sum()
        contingency[0, 1] = ((~activations) & (term_vec)).sum()
        contingency[1, 0] = ((activations) & (~term_vec)).sum()
        contingency[1, 1] = ((activations) & (term_vec)).sum()
        p_val = stats.chi2_contingency(contingency, False)[1]
        vox_z_val = normal.isf(p_val / 2)
        if activations[term_vec].mean() < activations[~term_vec].mean():
            vox_z_val = -vox_z_val
        stats_z_vals.append(vox_z_val)
    assert np.allclose(z_vals, stats_z_vals)


def test_fit_neurosynth(extracted_data_dir, tfidf_dir):
    output_dir, code = _fit_neurosynth.fit_neurosynth(
        tfidf_dir, extracted_data_dir, n_jobs=2
    )
    assert code == ExitCode.COMPLETED
    assert output_dir.joinpath("app.py").is_file()
    assert list(output_dir.joinpath("neurosynth_maps").glob("*.nii.gz"))


def test_does_not_rerun(tmp_path, monkeypatch):
    tmp_path.joinpath("info.json").write_text(
        json.dumps({"is_complete": True}),
        "utf-8",
    )
    mock = Mock()
    monkeypatch.setattr("nqdc._fit_neurosynth._do_fit_neurosynth", mock)
    _, code = _fit_neurosynth.fit_neurosynth(tmp_path, tmp_path, tmp_path)
    assert code == ExitCode.COMPLETED
    assert len(mock.mock_calls) == 0
