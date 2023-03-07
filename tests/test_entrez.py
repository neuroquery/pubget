import json
from unittest.mock import MagicMock, Mock

import pytest
from lxml import etree

from pubget import _entrez


def test_esearch(entrez_mock, tmp_path):
    client = _entrez.EntrezClient()
    client.esearch("fmri")
    client.efetch(output_dir=tmp_path, n_docs=10, retmax=3)
    i = None
    for i, batch in enumerate(sorted(tmp_path.glob("*.xml"))):
        batch = etree.parse(str(batch)).getroot()
        assert batch.tag == "pmc-articleset"
    assert i == min(10, entrez_mock.count) // 3


def test_epost(entrez_mock):
    client = _entrez.EntrezClient()
    assert client.epost([]) == {}
    result = client.epost([1, 10, 78])
    assert result["webenv"] == "WEBENV_1"
    assert result["querykey"] == "1"
    assert result["count"] == "3"


def test_entrez_api_key(entrez_mock):
    client = _entrez.EntrezClient()
    client.esearch("fmri")
    req = entrez_mock.last_request
    assert "api_key" not in req.body
    assert "api_key" not in req.url
    client = _entrez.EntrezClient(api_key="MYAPIKEY")
    client.esearch("fmri")
    req = entrez_mock.last_request
    assert "api_key=MYAPIKEY" in req.body
    assert "api_key" not in req.url


def test_epost_failure(requests_mock):
    requests_mock.side_effect = RuntimeError
    client = _entrez.EntrezClient()
    res = client.epost([1, 2, 3])
    assert res == {}
    assert client.n_failures == 1
    assert client.last_search_result is None

    resp = Mock()
    requests_mock.side_effect = None
    resp.content = "<broken>xml<"
    requests_mock.return_value = resp
    res = client.epost([1, 2, 3])
    assert res == {}
    assert client.n_failures == 1
    assert client.last_search_result is None


def test_esearch_failure(requests_mock, tmp_path):
    requests_mock.side_effect = RuntimeError
    client = _entrez.EntrezClient()
    res = client.esearch("fmri")
    assert res == {}
    assert client.n_failures == 1
    assert client.last_search_result is None

    resp = Mock()
    requests_mock.side_effect = None
    resp.json.side_effect = ValueError
    requests_mock.return_value = resp
    res = client.esearch("fmri")
    assert res == {}
    assert client.n_failures == 1
    assert client.last_search_result is None

    resp.json.side_effect = None
    resp.json.return_value = {"esearchresult": {"ERROR": "too many requests"}}
    res = client.esearch("fmri")
    assert res == {}
    assert client.n_failures == 1
    assert client.last_search_result is None

    client.n_failures = 0
    client.efetch(tmp_path)
    assert client.n_failures == 1


def test_dump_failed_request(tmp_path):
    client = _entrez.EntrezClient()
    req_mock, resp_mock = Mock(), Mock()
    req_mock.headers = {"a": "b"}
    req_mock.body = "some content"
    req_mock.url = "http://example"
    resp_mock.url = "http://example"
    resp_mock.content = b"some content"
    resp_mock.headers = {"key": "val"}
    client._dump_failed_request_info(req_mock, resp_mock)
    client = _entrez.EntrezClient(
        failed_requests_dump_dir=tmp_path, api_key="myapikey"
    )
    client._dump_failed_request_info(req_mock, None)
    client._dump_failed_request_info(req_mock, resp_mock)
    req_dir = sorted(tmp_path.glob("*"))[-1]
    for file_name in (
        "request_url",
        "request_headers",
        "request_body",
        "response_url",
        "response_headers",
        "response_content",
    ):
        assert req_dir.joinpath(file_name).is_file()
    assert "myapikey" not in req_dir.joinpath("request_body").read_text(
        "utf-8"
    )
    resp_mock = Mock()
    resp_mock.content.side_effect = Exception
    client._dump_failed_request_info(req_mock, resp_mock)


@pytest.mark.parametrize(
    ("service", "bad_resp", "good_resp", "args"),
    [
        (
            "esearch",
            b"",
            b'{"esearchresult": {"count": "", "webenv": "", "querykey": ""}}',
            ("fmri",),
        ),
        (
            "esearch",
            b'{"esearchresult": {"ERROR": "too many requests"}}',
            b'{"esearchresult": {"count": "", "webenv": "", "querykey": ""}}',
            ("fmri",),
        ),
        (
            "esearch",
            b'{"esearchresult": {"count": "", "webenv": "", "missing": ""}}',
            b'{"esearchresult": {"count": "", "webenv": "", "querykey": ""}}',
            ("fmri",),
        ),
        (
            "efetch",
            b"",
            b"<pmc-articleset><article/></pmc-articleset>",
            ("tmp_path", {"webenv": "", "querykey": "", "count": 3}),
        ),
    ],
)
def test_retry(tmp_path, requests_mock, service, bad_resp, good_resp, args):
    resp_0 = Mock()
    resp_0.status_code = 300
    resp_1 = Mock()
    resp_1.status_code = 200
    resp_1.content = bad_resp
    resp_2 = Mock()
    resp_2.status_code = 200
    resp_2.content = good_resp
    responses = [resp_0, resp_1, resp_2, Mock(), Mock()]
    for resp in responses:
        try:
            resp.json.return_value = json.loads(resp.content.decode("UTF-8"))
        except (TypeError, ValueError):
            pass
    requests_mock.side_effect = responses
    client = _entrez.EntrezClient(request_period=0.0)
    args = [tmp_path if a == "tmp_path" else a for a in args]
    getattr(client, service)(*args)
    assert requests_mock.call_count == 3


def test_epost_retry(tmp_path, requests_mock):
    resp_0 = Mock()
    resp_0.status_code = 200
    resp_0.content = b""
    resp_1 = Mock()
    resp_1.status_code = 200
    resp_1.content = (
        b"<resp><WebEnv>WEBENV_1</WebEnv><QueryKey>1</QueryKey></resp>"
    )
    resp_2 = Mock()
    resp_2.status_code = 200
    content = {
        "esearchresult": {"count": "2", "webenv": "WEBENV_1", "querykey": "2"}
    }
    resp_2.content = json.dumps(content).encode("UTF-8")
    resp_2.json.return_value = content
    responses = [resp_0, resp_1, resp_2, Mock(), Mock()]
    requests_mock.side_effect = responses
    client = _entrez.EntrezClient(request_period=0.0)
    client.epost([123, 456])
    assert requests_mock.call_count == 3
