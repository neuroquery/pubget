from unittest.mock import Mock
import json
import math

import pytest

from nqdc import _labelbuddy, _download, _articles, _data_extraction


def test_does_not_rerun(tmp_path, monkeypatch):
    tmp_path.joinpath("info.json").write_text(
        json.dumps({"is_complete": True, "name": "labelbuddy_data_creation"}),
        "utf-8",
    )
    mock = Mock()
    monkeypatch.setattr("nqdc._nimare._collect_nimare_data", mock)
    _, code = _labelbuddy.make_labelbuddy_documents(tmp_path, tmp_path)
    assert code == 0
    assert len(mock.mock_calls) == 0


def test_bad_part_size(tmp_path):
    for part_size in [0, -1, -2]:
        with pytest.raises(ValueError):
            _labelbuddy.make_labelbuddy_documents(
                tmp_path, tmp_path, part_size=part_size
            )


@pytest.mark.parametrize("part_size", [3, None, 1000])
def test_make_labelbuddy_documents(
    part_size, tmp_path, entrez_mock, monkeypatch
):
    monkeypatch.setattr(_labelbuddy, "_CHAPTER_SIZE", 2)
    monkeypatch.setattr(_labelbuddy, "_LOG_FREQUENCY", 2)
    download_dir, _ = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path.joinpath("query-abc")
    )
    articles_dir, _ = _articles.extract_articles(download_dir)
    data_dir, _ = _data_extraction.extract_data_to_csv(articles_dir)
    labelbuddy_dir, code = _labelbuddy.make_labelbuddy_documents(
        data_dir, part_size=part_size
    )
    assert code == 0
    assert labelbuddy_dir.name == "subset_allArticles_labelbuddyData"
    n_articles = json.loads(
        articles_dir.joinpath("info.json").read_text("utf-8")
    )["n_articles"]
    expected_part_size = (
        n_articles if part_size is None else min(part_size, n_articles)
    )
    assert len(list(labelbuddy_dir.glob("*.jsonl"))) == math.ceil(
        n_articles / expected_part_size
    )
    with open(
        labelbuddy_dir.joinpath("documents_00001.jsonl"), "r", encoding="utf-8"
    ) as f:
        docs = [json.loads(doc_json) for doc_json in f]
    assert len(docs) == expected_part_size
    assert any(
        "Abstract\n The abstract of the article" in d["text"] for d in docs
    )
    assert [d["meta"]["chapter"] for d in docs] == [
        i // 2 + 1 for i in range(len(docs))
    ]


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
