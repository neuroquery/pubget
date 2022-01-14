from pathlib import Path
import json
from urllib.parse import urlparse

from lxml import etree
import pytest


@pytest.fixture(autouse=True)
def request_mocker(monkeypatch):
    mock_entrez = MockEntrez()
    monkeypatch.setattr("requests.sessions.Session.send", mock_entrez)
    return mock_entrez


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


class MockEntrez:
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
        parsed = urlparse(request.url)
        params = _parse_query(parsed.query)
        batch_nb = int(params["retstart"]) // 3
        batch_file = Path(__file__).parent.joinpath(
            "data", "article_sets", f"batch_{batch_nb}.xml"
        )
        if not batch_file.exists():
            return Response(status_code=400, reason="Bad Request")
        content = batch_file.read_bytes()
        return Response(request.url, content=content)
