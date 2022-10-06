"""Extracting coordinate space from articles, based on NeuroSynth heuristic."""
import pathlib
import re
from typing import Any, Dict

from lxml import etree

from pubget._typing import Extractor
from pubget._utils import get_pmcid


class CoordinateSpaceExtractor(Extractor):
    """Extracting coordinate space from article XML"""

    fields = ("pmcid", "coordinate_space")
    name = "coordinate_space"

    def extract(
        self, article: etree.ElementTree, article_dir: pathlib.Path
    ) -> Dict[str, Any]:
        return {
            "pmcid": get_pmcid(article),
            "coordinate_space": _neurosynth_guess_space(
                " ".join(article.xpath(".//text()"))
            ),
        }


def _neurosynth_guess_space(text: str) -> str:
    (
        "adapted from https://github.com/neurosynth/ACE/"
        "blob/4e1d8fa8f924547a41c6d8b79cbd9ef9ffef14c2/ace/extract.py#L7"
    )
    text = text.lower()
    found = {}
    for term in [
        "mni",
        "talairach",
        "spm",
        "fsl",
        "afni",
        "brainvoyager",
    ]:
        found[term] = re.search(rf"\b{term}.{{0,20}}?\b", text) is not None
    found["mni_software"] = found["spm"] or found["fsl"]
    found["talairach_software"] = found["afni"] or found["brainvoyager"]
    found["any_software"] = (
        found["mni_software"] or found["talairach_software"]
    )
    if found["mni_software"] and not found["talairach_software"]:
        return "MNI"
    if found["mni"] and not found["talairach"] and not found["any_software"]:
        return "MNI"
    if found["talairach_software"] and not found["mni_software"]:
        return "TAL"
    if found["talairach"] and not found["mni"] and not found["any_software"]:
        return "TAL"
    return "UNKNOWN"
