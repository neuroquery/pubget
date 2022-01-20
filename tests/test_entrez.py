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
