import pathlib
import pytest
from lxml import etree

from nqdc import _links


@pytest.mark.parametrize("n_links", [0, 1, 5])
def test_link_extractor(n_links):
    xml_template = b"""<?xml version='1.0' encoding='ASCII'?>
<article xmlns:xlink="http://www.w3.org/1999/xlink">
    <front>
        <article-meta>
            <article-id pub-id-type="pmc">9057060</article-id>
        </article-meta>
    </front>
    <body>
    some links:
    %s
    </body>
</article>
    """
    one_link = (
        b'link <ext-link xlink:href="http:example.com/%d">'
        b"http:example.com</ext-link>"
    )
    links_text = b"\n".join([one_link % i for i in range(n_links)])
    xml = xml_template % links_text
    document = etree.ElementTree(etree.XML(xml))
    extracted = _links.LinkExtractor().extract(
        document, pathlib.Path("pmc_9057060")
    )
    assert extracted.shape == (n_links, 3)
