from lxml import etree

from nqdc import _entrez


def test_entrez(entrez_mock):
    client = _entrez.EntrezClient()
    client.esearch("fmri")
    for i, batch in enumerate(client.efetch(n_docs=10, retmax=3)):
        batch = etree.XML(batch)
        assert batch.tag == "pmc-articleset"
    assert i == min(10, entrez_mock.count) // 3
