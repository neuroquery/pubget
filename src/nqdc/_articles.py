import json
import logging
from pathlib import Path
from typing import Generator, Tuple, Optional

from lxml import etree

from nqdc import _utils
from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


def extract_articles(
    input_dir: PathLikeOrStr, output_dir: Optional[PathLikeOrStr] = None
) -> Tuple[Path, int]:
    input_dir = Path(input_dir)
    download_complete = _check_if_download_complete(input_dir)
    if output_dir is None:
        output_dir = input_dir.parent.joinpath("articles")
    else:
        output_dir = Path(output_dir)
    _LOG.info(f"Extracting articles from {input_dir} to {output_dir}")
    output_dir.mkdir(exist_ok=True, parents=True)
    n_articles = 0
    for batch_file in sorted(input_dir.glob("batch_*.xml")):
        _LOG.debug(f"Extracting articles from {batch_file.name}")
        for (pmcid, article) in _extract_from_articleset(batch_file):
            subdir = output_dir.joinpath(_utils.checksum(str(pmcid))[:3])
            subdir.mkdir(exist_ok=True, parents=True)
            target_file = subdir.joinpath(f"pmcid_{pmcid}.xml")
            with open(target_file, "wb") as f:
                article.write(f, encoding="UTF-8", xml_declaration=True)
            n_articles += 1
    _LOG.info(
        f"Extracted {n_articles} articles from {input_dir} to {output_dir}"
    )
    return output_dir, int(not download_complete)


def _check_if_download_complete(download_dir: Path) -> bool:
    try:
        complete = json.loads(
            download_dir.joinpath("info.json").read_text("utf-8")
        )["download_complete"]
        if complete:
            return True
    except Exception:
        pass
    _LOG.warning("Not all articles for the query have been downloaded")
    return False


def _extract_from_articleset(
    batch_file: Path,
) -> Generator[Tuple[int, etree.ElementTree], None, None]:
    with open(batch_file, "rb") as f:
        try:
            tree = etree.parse(f)
        except Exception:
            _LOG.exception(f"Bad article set file: {batch_file.name}")
        for art_nb, article in enumerate(tree.iterfind("article")):
            try:
                pmcid = int(
                    article.xpath(
                        "front/article-meta/"
                        "article-id[@pub-id-type='pmc']/text()"
                    )[0]
                )
            except Exception:
                _LOG.error(
                    f"Could not find pmcid for article {art_nb} "
                    f"of {batch_file.name}"
                )
            else:
                yield pmcid, etree.ElementTree(article)
