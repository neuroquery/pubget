import logging
import argparse
from pathlib import Path
from typing import Tuple, Optional, Mapping

from lxml import etree
from joblib import Parallel, delayed

from nqdc import _utils
from nqdc._typing import PathLikeOrStr, BaseProcessingStep

_LOG = logging.getLogger(__name__)


def extract_articles(
    articlesets_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    n_jobs: int = 1,
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
    n_jobs
        Number of processes to run in parallel. `-1` means using all
        processors.

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
    if output_dir is None:
        output_dir = articlesets_dir.with_name("articles")
    else:
        output_dir = Path(output_dir)
    status = _utils.check_steps_status(articlesets_dir, output_dir, __name__)
    if not status["need_run"]:
        return output_dir, 0
    _LOG.info(f"Extracting articles from {articlesets_dir} to {output_dir}")
    output_dir.mkdir(exist_ok=True, parents=True)
    n_jobs = _utils.check_n_jobs(n_jobs)
    n_articles = _do_extract_articles(articlesets_dir, output_dir, n_jobs)
    _LOG.info(
        f"Extracted {n_articles} articles from "
        f"{articlesets_dir} to {output_dir}"
    )
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(
        output_dir,
        name="article_extraction",
        is_complete=is_complete,
        n_articles=n_articles,
    )
    return output_dir, int(not is_complete)


def _do_extract_articles(
    articlesets_dir: Path, output_dir: Path, n_jobs: int
) -> int:
    """Do the extraction and return number of articles found."""
    output_dir.mkdir(exist_ok=True, parents=True)
    article_counts = Parallel(n_jobs=n_jobs, verbose=8)(
        delayed(_extract_from_articleset)(batch_file, output_dir=output_dir)
        for batch_file in articlesets_dir.glob("articleset_*.xml")
    )
    return sum(article_counts)


def _extract_from_articleset(batch_file: Path, output_dir: Path) -> int:
    """Extract articles from one batch and return the number of articles."""
    _LOG.debug(f"Extracting articles from {batch_file.name}")
    with open(batch_file, "rb") as f:
        tree = etree.parse(f)
    n_articles = 0
    for art_nb, article in enumerate(tree.iterfind("article")):
        pmcid = _utils.get_pmcid(article)
        subdir = output_dir.joinpath(_utils.checksum(str(pmcid))[:3])
        subdir.mkdir(exist_ok=True, parents=True)
        target_file = subdir.joinpath(f"pmcid_{pmcid}.xml")
        target_file.write_bytes(
            etree.tostring(article, encoding="UTF-8", xml_declaration=True)
        )
        n_articles += 1
    return n_articles


class ArticleExtractionStep(BaseProcessingStep):
    name = "article_extraction"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        _utils.add_n_jobs_argument(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        download_dir = previous_steps_output["download"]
        return extract_articles(download_dir, n_jobs=args.n_jobs)


class StandaloneArticleExtractionStep(BaseProcessingStep):
    name = "article_extraction"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        argument_parser.add_argument(
            "articlesets_dir",
            help="Directory from which to extract articles. It is a directory "
            "created by the nqdc_download command. "
            "A sibling directory will be "
            "created to contain the individual article files",
        )
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = (
            "Extract articles from batches (articleset XML files) "
            "downloaded from PubMed Central by the nqdc_download command."
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        download_dir = Path(args.articlesets_dir)
        return extract_articles(download_dir, n_jobs=args.n_jobs)
