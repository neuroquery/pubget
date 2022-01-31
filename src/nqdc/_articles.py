import json
import logging
from pathlib import Path
from typing import Generator, Tuple, Optional

from lxml import etree

from nqdc import _utils
from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


def extract_articles(
    articlesets_dir: PathLikeOrStr, output_dir: Optional[PathLikeOrStr] = None
) -> Tuple[Path, int]:
    """Extract articles from bulk download files.

    Parameters
    ----------
    articlesets_dir
        Directory containing the downloaded files. It is a directory created by
        `nqdc.download_articles_for_query`: it is named `articlesets` and it
        contains the bulk download files `articleset_00000.xml`,
        `articleset_00001.xml`, etc.
    output_dir
        Directory where to store the extracted articles. If not specified, a
        sibling directory of `articlesets_dir` called `articles` will be used.

    Returns
    -------
    output_dir
        The directory in which articles are stored. To avoid having a very
        large number of files in one directory, subdirectories with names
        ranging from `000` to `fff` are created. Each article is stored in the
        subdirectory that matches the first hexadecimal digits of the md5
        checksum of its PMC id. Therefore the contents of the `articles`
        directory might look like:
        ```
        · articles
          ├── 001
          │   └── pmcid_4150635.xml
          └── 00b
              ├── pmcid_2568959.xml
              └── pmcid_5102699.xml
        ```
    exit_code
        0 if the download in `articlesets_dir` was complete and the article
        extraction finished normally and 1 otherwise. Used by the `nqdc`
        command-line interface.
    """
    articlesets_dir = Path(articlesets_dir)
    _utils.assert_exists(articlesets_dir)
    download_complete = _utils.is_step_complete(articlesets_dir, "download")
    if not download_complete:
        _LOG.warning("Not all articles for the query have been downloaded.")
    if output_dir is None:
        output_dir = articlesets_dir.with_name("articles")
    else:
        output_dir = Path(output_dir)
    _LOG.info(f"Extracting articles from {articlesets_dir} to {output_dir}")
    output_dir.mkdir(exist_ok=True, parents=True)
    if _utils.is_step_complete(output_dir, "article_extraction"):
        _LOG.info("Article extraction already complete, nothing to do.")
        return output_dir, 0
    n_articles = _do_extract_articles(articlesets_dir, output_dir)
    _LOG.info(
        f"Extracted {n_articles} articles from "
        f"{articlesets_dir} to {output_dir}"
    )
    info = {
        "article_extraction_complete": download_complete,
        "n_articles": n_articles,
    }
    output_dir.joinpath("info.json").write_text(
        json.dumps(info), encoding="utf-8"
    )
    return output_dir, int(not download_complete)


def _do_extract_articles(articlesets_dir: Path, output_dir: Path) -> int:
    n_articles = 0
    for batch_file in sorted(articlesets_dir.glob("articleset_*.xml")):
        _LOG.debug(f"Extracting articles from {batch_file.name}")
        for (pmcid, article) in _extract_from_articleset(batch_file):
            subdir = output_dir.joinpath(_utils.checksum(str(pmcid))[:3])
            subdir.mkdir(exist_ok=True, parents=True)
            target_file = subdir.joinpath(f"pmcid_{pmcid}.xml")
            with open(target_file, "wb") as f:
                article.write(f, encoding="UTF-8", xml_declaration=True)
            n_articles += 1
    return n_articles


def _extract_from_articleset(
    batch_file: Path,
) -> Generator[Tuple[int, etree.ElementTree], None, None]:
    with open(batch_file, "rb") as f:
        tree = etree.parse(f)
    for art_nb, article in enumerate(tree.iterfind("article")):
        pmcid = _utils.get_pmcid(article)
        yield pmcid, etree.ElementTree(article)
