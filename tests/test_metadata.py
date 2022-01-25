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
    metadata = _metadata.MetadataExtractor()(xml)
    assert metadata["pmcid"] == 123
    assert metadata["pmid"] == 456
    assert metadata["doi"] == "doi789"
    assert metadata["title"] == "The title"
    assert metadata["journal"] == "My Journal"
    assert metadata["publication_year"] == 1999
