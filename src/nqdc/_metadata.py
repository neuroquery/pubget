from typing import Dict, Any

from lxml import etree

from nqdc._typing import BaseExtractor


class MetadataExtractor(BaseExtractor):
    fields = ("pmcid", "pmid", "doi", "title", "journal", "publication_year")
    name = "metadata"

    def extract(self, article: etree.ElementTree) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        for article_id in article.iterfind("front/article-meta/article-id"):
            _add_id(article_id, metadata)
        title_elem = article.find(
            "front/article-meta/title-group/article-title"
        )
        if title_elem is not None:
            metadata["title"] = "".join(title_elem.xpath(".//text()"))
        _add_journal(article, metadata)
        _add_pub_date(article, metadata)
        return metadata


def _add_journal(article: etree.Element, metadata: Dict[str, Any]) -> None:
    journal_elem = article.find(
        "front/journal-meta/journal-id[@journal-id-type='nlm-ta']"
    )
    if journal_elem is not None:
        metadata["journal"] = journal_elem.text


def _add_pub_date(article: etree.Element, metadata: Dict[str, Any]) -> None:
    pub_date_elems = article.findall("front/article-meta/pub-date/year")
    pub_dates = []
    for elem in pub_date_elems:
        try:
            if len(elem.text) == 4:
                pub_dates.append(int(elem.text))
        except Exception:
            pass
    if pub_dates:
        metadata["publication_year"] = min(pub_dates)


def _add_id(article_id: etree.Element, metadata: Dict[str, Any]) -> None:
    id_type = article_id.get("pub-id-type")
    if id_type not in ["pmc", "pmid", "doi"]:
        return
    if id_type == "pmc":
        id_type = "pmcid"
    value = article_id.text
    if id_type in ["pmid", "pmcid"]:
        value = int(value)
    metadata[id_type] = value
