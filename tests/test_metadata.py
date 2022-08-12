import pytest
from lxml import etree

from nqdc import _metadata


def test_metadata_extractor():
    xml = etree.XML(
        """<article>
    <front>
    <journal-meta>
    <journal-id journal-id-type="nlm-ta">My Journal</journal-id>
    </journal-meta>
    <article-meta>
    <title-group>
    <article-title>The title</article-title>
    </title-group>
    <article-id pub-id-type="publisher">10</article-id>
    <article-id pub-id-type="pmc">123</article-id>
    <article-id pub-id-type="pmid">456</article-id>
    <article-id pub-id-type="doi">doi789</article-id>
    <pub-date><year/></pub-date>
    <pub-date><year>last year</year></pub-date>
    <pub-date><year>98</year></pub-date>
    <pub-date><year>1999</year></pub-date>
    </article-meta>
    </front>
    </article>
    """
    )
    metadata = _metadata.MetadataExtractor().extract(xml)
    assert metadata["pmcid"] == 123
    assert metadata["pmid"] == 456
    assert metadata["doi"] == "doi789"
    assert metadata["title"] == "The title"
    assert metadata["journal"] == "My Journal"
    assert metadata["publication_year"] == 1999


@pytest.mark.parametrize(
    "loc", ["href", "ext-link", "uri", "ali", "type", None]
)
def test_add_license(loc):
    article_tpl = """<article><front><article-meta>
    <permissions>{}</permissions></article-meta></front></article>"""
    if loc == "href":
        license_elem = (
            "<license xmlns:xlink='http://www.w3.org/1999/xlink' "
            "xlink:href='mylicense' />"
        )
    elif loc == "ext-link":
        license_elem = (
            "<license xmlns:xlink='http://www.w3.org/1999/xlink'>"
            "<p>some text <ext-link xlink:href='mylicense'>text</ext-link></p>"
            "</license>"
        )
    elif loc == "uri":
        license_elem = (
            "<license xmlns:xlink='http://www.w3.org/1999/xlink'>"
            "<p>some text <uri xlink:href='mylicense'>text</uri></p>"
            "</license>"
        )
    elif loc == "ali":
        license_elem = (
            "<license xmlns:ali='http://www.niso.org/schemas/ali/1.0/'>"
            "<p>some text <ali:license_ref>mylicense</ali:license_ref></p>"
            "</license>"
        )
    elif loc == "type":
        license_elem = "<license license-type='mylicense'/>"
    elif loc is None:
        license_elem = ""
    article = article_tpl.format(license_elem)
    meta = {}
    _metadata._add_license(etree.XML(article), meta)
    if loc is not None:
        assert meta["license"] == "mylicense"
    else:
        assert "license" not in meta
