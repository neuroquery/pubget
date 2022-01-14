from lxml import etree

from nqdc import _entrez


def test_entrez():
    client = _entrez.EntrezClient()
    client.esearch("fmri")
    for batch in client.efetch(n_docs=10, retmax=3):
        batch = etree.XML(batch)
        assert batch.tag == "pmc-articleset"
