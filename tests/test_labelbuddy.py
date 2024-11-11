import json
import math
from unittest.mock import Mock

import pandas as pd
import pytest

from pubget import (
    ExitCode,
    _articles,
    _data_extraction,
    _download,
    _labelbuddy,
)


def test_does_not_rerun(tmp_path, monkeypatch):
    tmp_path.joinpath("info.json").write_text(
        json.dumps({"is_complete": True, "name": "labelbuddy_data_creation"}),
        "utf-8",
    )
    mock = Mock()
    monkeypatch.setattr("pubget._nimare._collect_nimare_data", mock)
    _, code = _labelbuddy.make_labelbuddy_documents(tmp_path, tmp_path)
    assert code == ExitCode.COMPLETED
    assert len(mock.mock_calls) == 0


def test_bad_batch_size(tmp_path):
    for batch_size in [0, -1, -2]:
        with pytest.raises(ValueError):
            _labelbuddy.make_labelbuddy_documents(
                tmp_path, tmp_path, batch_size=batch_size
            )


@pytest.mark.parametrize("batch_size", [3, None, 1000, "n_docs"])
def test_make_labelbuddy_documents(
    batch_size, tmp_path, entrez_mock, monkeypatch
):
    monkeypatch.setattr(_labelbuddy, "_LOG_PERIOD", 2)
    download_dir, _ = _download.download_query_results(
        "fMRI[abstract]", tmp_path.joinpath("query_abc")
    )
    articles_dir, _ = _articles.extract_articles(download_dir)
    data_dir, _ = _data_extraction.extract_data_to_csv(articles_dir)
    n_articles = json.loads(
        articles_dir.joinpath("info.json").read_text("utf-8")
    )["n_articles"]
    if batch_size == "n_docs":
        batch_size = n_articles
    labelbuddy_dir, code = _labelbuddy.make_labelbuddy_documents(
        data_dir, batch_size=batch_size
    )
    assert code == ExitCode.COMPLETED
    assert labelbuddy_dir.name == "subset_allArticles_labelbuddyData"
    expected_batch_size = (
        n_articles if batch_size is None else min(batch_size, n_articles)
    )
    assert len(list(labelbuddy_dir.glob("*.jsonl"))) == math.ceil(
        n_articles / expected_batch_size
    )
    with open(
        labelbuddy_dir.joinpath("documents_00001.jsonl"), "r", encoding="utf-8"
    ) as f:
        docs = [json.loads(doc_json) for doc_json in f]
    assert len(docs) == expected_batch_size
    assert all("Body\n The text of" in d["text"] for d in docs)
    _check_batch_info(labelbuddy_dir)


def _check_batch_info(labelbuddy_dir):
    batch_info = pd.read_csv(labelbuddy_dir.joinpath("batch_info.csv"))
    for file_name, file_contents in batch_info.groupby("file_name"):
        docs = [
            json.loads(doc_line)
            for doc_line in labelbuddy_dir.joinpath(file_name)
            .read_text("utf-8")
            .strip()
            .split("\n")
        ]
        for pmcid, _, line in file_contents.values:
            assert pmcid == docs[line]["metadata"]["pmcid"]


@pytest.mark.parametrize(
    ("template", "fields"),
    [
        (
            _labelbuddy._TEMPLATE,
            {
                "authors": ",",
                "body": "the text",
                "abstract": "abcd",
                "keywords": "",
                "title": "fmri",
                "publication_year": 2000,
                "journal": "Journ. Brain. Imag.",
                "tables": "x\ty\tz",
            },
        ),
        (
            "{f_0} something, {f_1}{f_2}",
            {"f_0": "abc ", "f_1": "\n", "f_2": "α"},
        ),
    ],
)
def test_get_inserted_field_positions(template, fields):
    formatted = template.format(**fields)
    positions = _labelbuddy._get_inserted_field_positions(template, fields)
    inserted = {
        field_name: formatted[start:end]
        for (field_name, (start, end)) in positions.items()
    }
    assert inserted == {k: str(v) for k, v in fields.items()}
