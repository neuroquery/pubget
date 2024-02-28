"""Extracting all URLs from an article XML."""
import pathlib
import re
from typing import Dict, Tuple

import pandas as pd
from lxml import etree

from pubget import _utils
from pubget._typing import Extractor, Records


class LinkExtractor(Extractor):
    """Extracting all URLs in an article.

    Currently only ext-link elements are considered, but this may change in the
    future.
    """

    fields = ("id", "ext-link-type", "href")
    name = "links"

    def extract(
        self,
        article: etree.ElementTree,
        article_dir: pathlib.Path,
        previous_extractors_output: Dict[str, Records],
    ) -> pd.DataFrame:
        del article_dir, previous_extractors_output
        id = _utils.get_id(article)
        all_links = []
        xlink = "http://www.w3.org/1999/xlink"
        for tag in ["uri", "ext-link"]:
            for link in article.iterfind(f"//{tag}[@{{{xlink}}}href]"):
                href = link.get(f"{{{xlink}}}href")
                link_type = link.get("ext-link-type") or tag
                all_links.append(
                    {"id": id, "ext-link-type": link_type, "href": href}
                )
        return pd.DataFrame(all_links, columns=self.fields).drop_duplicates()


class LinkContentExtractor(Extractor):
    """Extracting values from link hrefs.

    the `name` is the Extractor's name and naming capture groups in the pattern
    define the fields of the extracted records. Records with null values and
    duplicates are dropped.
    """

    def __init__(self, pattern: str, name: str) -> None:
        self.name = name
        self.pattern = pattern
        capture_groups = re.findall(r"\(\?P<(\w+)>", self.pattern)
        self.fields = ("id", *capture_groups)

    def extract(
        self,
        article: etree.ElementTree,
        article_dir: pathlib.Path,
        previous_extractors_output: Dict[str, Records],
    ) -> pd.DataFrame:
        del article, article_dir
        links = previous_extractors_output.get("links")
        if links is None or (len(links) == 0):
            return pd.DataFrame(columns=self.fields)
        captured = links["href"].str.extract(self.pattern, expand=True)
        captured["id"] = links["id"]
        return pd.DataFrame(
            captured.dropna().drop_duplicates().reset_index(),
            columns=self.fields,
        )


def neurovault_id_extractors() -> Tuple[Extractor, Extractor]:
    """Get a pair of extractors, for collection and image IDs respectively."""
    return (
        LinkContentExtractor(
            r"""(?x)
            .*(?:neurovault.org/collections/
            |neurovault.org/api/collections/
            |identifiers.org/neurovault.collection:)
            (?P<collection_id>\w+)
            """,
            "neurovault_collections",
        ),
        LinkContentExtractor(
            r"""(?x)
            .*(?:neurovault.org/images/
            |neurovault.org/api/images/
            |identifiers.org/neurovault.image:)
            (?P<image_id>\d+)
            """,
            "neurovault_images",
        ),
    )
