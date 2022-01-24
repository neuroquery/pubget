from unittest.mock import Mock

from lxml import etree

from nqdc import _entrez


def test_entrez(entrez_mock, tmp_path):
    client = _entrez.EntrezClient()
    client.esearch("fmri")
    client.efetch(output_dir=tmp_path, n_docs=10, retmax=3)
    for i, batch in enumerate(sorted(tmp_path.glob("*.xml"))):
        batch = etree.parse(str(batch)).getroot()
        assert batch.tag == "pmc-articleset"
    assert i == min(10, entrez_mock.count) // 3


def test_entrez_api_key(requests_mock):
    client = _entrez.EntrezClient()
    client.esearch("fmri")
    req = requests_mock.call_args_list[0][0][0]
    assert "api_key" not in req.body
    assert "api_key" not in req.url
    client = _entrez.EntrezClient(api_key="MYAPIKEY")
    client.esearch("fmri")
    req = requests_mock.call_args_list[1][0][0]
    assert "api_key=MYAPIKEY" in req.body
    assert "api_key" not in req.url


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

    client.n_failures = 0
    client.efetch(tmp_path, {"webenv": "some webenv", "querykeymissing": ""})
    assert client.n_failures == 1
