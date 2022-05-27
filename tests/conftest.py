import os
import shutil
from pathlib import Path
import itertools
import json
from unittest.mock import Mock

import numpy as np
import pandas as pd
from scipy import sparse
from lxml import etree
import pytest


@pytest.fixture(autouse=True)
def erase_config(monkeypatch):
    for variable in os.environ:
        if variable.startswith("NQDC_"):
            monkeypatch.delenv(variable)


@pytest.fixture(autouse=True)
def requests_mock(monkeypatch):
    mock = Mock()
    monkeypatch.setattr("requests.sessions.Session.send", mock)
    return mock


@pytest.fixture()
def entrez_mock(monkeypatch):
    mock = EntrezMock()
    monkeypatch.setattr("requests.sessions.Session.send", mock)
    return mock


@pytest.fixture(scope="session")
def test_data_dir():
    return Path(__file__).with_name("data")


@pytest.fixture(autouse=True)
def basic_nq_datasets_mock(monkeypatch):
    monkeypatch.setattr("neuroquery.datasets.fetch_neuroquery_model", Mock)
    monkeypatch.setattr("nqdc._vectorization.fetch_neuroquery_model", Mock)


@pytest.fixture()
def nq_datasets_mock(test_data_dir, tmp_path, monkeypatch):
    nq_model_dir = tmp_path.joinpath("neuroquery_data", "neuroquery_model")
    nq_model_dir.mkdir(parents=True)
    shutil.copyfile(
        str(test_data_dir.joinpath("vocabulary.csv")),
        str(nq_model_dir.joinpath("vocabulary.csv")),
    )
    shutil.copyfile(
        str(
            test_data_dir.joinpath("vocabulary.csv_voc_mapping_identity.json")
        ),
        str(nq_model_dir.joinpath("vocabulary.csv_voc_mapping_identity.json")),
    )

    def fetch(*args, **kwargs):
        return str(nq_model_dir)

    monkeypatch.setattr("neuroquery.datasets.fetch_neuroquery_model", fetch)
    monkeypatch.setattr("nqdc._vectorization.fetch_neuroquery_model", fetch)


class Response:
    def __init__(self, url, content=b"", status_code=200, reason="OK"):
        self.url = url
        self.content = content
        self.status_code = status_code
        self.reason = reason

    def json(self):
        return json.loads(self.content.decode("utf-8"))


def _parse_query(query):
    result = {}
    for part in query.split("&"):
        key, value = part.split("=")
        result[key] = value
    return result


class EntrezMock:
    def __init__(self):
        batch_file = Path(__file__).parent.joinpath("data", "articleset.xml")
        self.article_set = etree.parse(str(batch_file))
        self.count = len(self.article_set.getroot())
        self.fail_efetch = False

    def __call__(self, request, *args, **kwargs):
        if "esearch.fcgi" in request.url:
            return self._esearch(request)
        if "efetch.fcgi" in request.url:
            return self._efetch(request)
        else:
            return Response(status_code=400, reason="Bad Request")

    def _esearch(self, request):
        response = {
            "esearchresult": {
                "count": "7",
                "retmax": "5",
                "retstart": "0",
                "querykey": "1",
                "webenv": "WEBENV_1",
            }
        }
        return Response(request.url, json.dumps(response).encode("utf-8"))

    def _efetch(self, request):
        params = _parse_query(request.body)
        retstart = int(params["retstart"])
        if self.fail_efetch and retstart != 0:
            return Response(
                request.url, status_code=500, reason="Internal Server Error"
            )
        retmax = int(params["retmax"])
        if retstart >= self.count:
            return Response(request.url, status_code=400, reason="Bad Request")
        result = etree.Element("pmc-articleset")
        start, end = retstart, retstart + retmax
        for article in self.article_set.getroot()[start:end]:
            result.append(article)
        content = etree.tostring(
            result, encoding="utf-8", xml_declaration=True
        )
        return Response(request.url, content=content)


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
    c_spaces_idx = list(set(coords.index.values))
    c_spaces_vals = list(
        itertools.islice(
            itertools.cycle(["TAL", "MNI", "UNKNOWN"]), len(c_spaces_idx)
        )
    )
    pd.DataFrame(
        {"pmcid": c_spaces_idx, "coordinate_space": c_spaces_vals}
    ).to_csv(extracted_dir.joinpath("coordinate_space.csv"), index=False)
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
