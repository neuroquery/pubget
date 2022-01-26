import os
import shutil
from pathlib import Path
import json
from unittest.mock import Mock

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
            return Response(status_code=500, reason="Internal Server Error")
        retmax = int(params["retmax"])
        if retstart >= self.count:
            return Response(status_code=400, reason="Bad Request")
        result = etree.Element("pmc-articleset")
        start, end = retstart, retstart + retmax
        for article in self.article_set.getroot()[start:end]:
            result.append(article)
        content = etree.tostring(
            result, encoding="utf-8", xml_declaration=True
        )
        return Response(request.url, content=content)
