import json
from unittest.mock import Mock

import neuroquery

from nqdc import ExitCode, _fit_neuroquery


def test_fit_neuroquery(extracted_data_dir, tfidf_dir):
    output_dir, code = _fit_neuroquery.fit_neuroquery(
        tfidf_dir, extracted_data_dir, n_jobs=2
    )
    assert code == ExitCode.COMPLETED
    model = neuroquery.NeuroQueryModel.from_data_dir(
        str(output_dir.joinpath("neuroquery_model"))
    )
    model("term 3 term 45 term 10000")
    assert output_dir.joinpath("app.py").is_file()
    # test re-running and overwriting previous model
    output_dir.joinpath("info.json").unlink()
    new_output_dir, code = _fit_neuroquery.fit_neuroquery(
        tfidf_dir, extracted_data_dir, n_jobs=2
    )
    assert code == ExitCode.COMPLETED
    assert new_output_dir == output_dir


def test_does_not_rerun(tmp_path, monkeypatch):
    tmp_path.joinpath("info.json").write_text(
        json.dumps({"is_complete": True}),
        "utf-8",
    )
    mock = Mock()
    monkeypatch.setattr("nqdc._fit_neuroquery._do_fit_neuroquery", mock)
    _, code = _fit_neuroquery.fit_neuroquery(tmp_path, tmp_path, tmp_path)
    assert code == ExitCode.COMPLETED
    assert len(mock.mock_calls) == 0
