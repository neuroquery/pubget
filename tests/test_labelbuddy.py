from unittest.mock import Mock
import json

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


def test_make_labelbuddy_documents(tmp_path, entrez_mock):
    download_dir, _ = _download.download_articles_for_query(
        "fMRI[abstract]", tmp_path.joinpath("query-abc")
    )
    articles_dir, _ = _articles.extract_articles(download_dir)
    data_dir, _ = _data_extraction.extract_data_to_csv(articles_dir)
    labelbuddy_dir, code = _labelbuddy.make_labelbuddy_documents(
        data_dir, batch_size=3
    )
    assert code == 0
    assert labelbuddy_dir.name == "subset_allArticles_labelbuddyData"
    assert len(list(labelbuddy_dir.glob("*.jsonl"))) == 3
    with open(
        labelbuddy_dir.joinpath("documents_00000.jsonl"), "r", encoding="utf-8"
    ) as f:
        docs = [json.loads(doc_json) for doc_json in f]
    assert len(docs) == 3
    assert "Abstract\n The abstract of the article" in docs[1]["text"]


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
