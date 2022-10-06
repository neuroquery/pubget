"""Extracting metatada from article XML."""
import pathlib
from typing import Any, Dict

from lxml import etree

from pubget._typing import Extractor


class MetadataExtractor(Extractor):
    """Extracting metatada from article XML."""

    fields = (
        "pmcid",
        "pmid",
        "doi",
        "title",
        "journal",
        "publication_year",
        "license",
    )
    name = "metadata"

    def extract(
        self, article: etree.ElementTree, article_dir: pathlib.Path
    ) -> Dict[str, Any]:
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
        _add_license(article, metadata)
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


def _add_license(article: etree.Element, metadata: Dict[str, Any]) -> None:
    license_elem = article.find("front/article-meta/permissions/license")
    if license_elem is None:
        return
    href = "{http://www.w3.org/1999/xlink}href"
    if href in license_elem.attrib:
        metadata["license"] = license_elem.get(href)
        return
    license_p_link = license_elem.find(".//ext-link")
    if license_p_link is None:
        license_p_link = license_elem.find(".//uri")
    if license_p_link is not None and href in license_p_link.attrib:
        metadata["license"] = license_p_link.get(href)
        return
    ali_link = license_elem.find(
        ".//{http://www.niso.org/schemas/ali/1.0/}license_ref"
    )
    if ali_link is not None:
        metadata["license"] = ali_link.text
        return
    if "license-type" in license_elem.attrib:
        metadata["license"] = license_elem.get("license-type")
        return


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
