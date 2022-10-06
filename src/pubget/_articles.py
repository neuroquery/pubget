"""'extract_articles' step: extract articles from bulk PMC download."""
import argparse
import json
import logging
from pathlib import Path
from typing import Generator, Mapping, Optional, Tuple

import pandas as pd
from joblib import Parallel, delayed
from lxml import etree

from pubget import _utils
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    PathLikeOrStr,
    PipelineStep,
)

_LOG = logging.getLogger(__name__)
_LOG_PERIOD = 500
_STEP_NAME = "extract_articles"
_STEP_DESCRIPTION = "Extract articles from bulk PMC download."


def extract_articles(
    articlesets_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    n_jobs: int = 1,
) -> Tuple[Path, ExitCode]:
    """Extract articles from bulk download files.

    Parameters
    ----------
    articlesets_dir
        Directory containing the downloaded files. It is a directory created by
        `pubget.download_articles_for_query`: it is named `articlesets` and it
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
          │   └── pmcid_4150635
          └── 00b
              ├── pmcid_2568959
              └── pmcid_5102699
        ```
        Each article gets its own subdirectory, containing the article's XML
        and its tables.
    exit_code
        COMPLETED if the download in `articlesets_dir` was complete and the
        article extraction finished normally and INCOMPLETE otherwise. Used by
        the `pubget` command-line interface.

    """
    articlesets_dir = Path(articlesets_dir)
    if output_dir is None:
        output_dir = articlesets_dir.with_name("articles")
    else:
        output_dir = Path(output_dir)
    status = _utils.check_steps_status(articlesets_dir, output_dir, __name__)
    if not status["need_run"]:
        return output_dir, ExitCode.COMPLETED
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
        name=_STEP_NAME,
        is_complete=is_complete,
        n_articles=n_articles,
    )
    exit_code = ExitCode.COMPLETED if is_complete else ExitCode.INCOMPLETE
    return output_dir, exit_code


def _do_extract_articles(
    articlesets_dir: Path, output_dir: Path, n_jobs: int
) -> int:
    """Do the extraction and return number of articles found."""
    output_dir.mkdir(exist_ok=True, parents=True)
    with Parallel(n_jobs=n_jobs, verbose=8) as parallel:
        _LOG.info("Extracting articles from PMC articlesets.")
        article_counts = parallel(
            delayed(_extract_from_articleset)(
                batch_file, output_dir=output_dir
            )
            for batch_file in articlesets_dir.glob("articleset_*.xml")
        )
        n_articles = int(sum(article_counts))  # int() is for mypy
        _LOG.info(
            f"Done extracting {n_articles} articles from PMC articlesets."
        )
        _LOG.info("Extracting tables from articles.")
        parallel(
            delayed(_extract_tables)(article_dir)
            for article_dir in _iter_articles(
                output_dir,
                f"Extracted tables from {{}} / {n_articles} articles.",
            )
        )
        _LOG.info("Done extracting tables from articles.")
    return n_articles


def _iter_articles(
    all_articles_dir: Path, message: str
) -> Generator[Path, None, None]:
    n_articles = 0
    for bucket in all_articles_dir.glob("*"):
        if bucket.is_dir():
            for article_dir in bucket.glob("pmcid_*"):
                n_articles += 1
                yield article_dir
                if not n_articles % _LOG_PERIOD:
                    _LOG.info(message.format(n_articles))


def _extract_from_articleset(batch_file: Path, output_dir: Path) -> int:
    """Extract articles from one batch and return the number of articles."""
    _LOG.debug(f"Extracting articles from {batch_file.name}")
    with open(batch_file, "rb") as batch_fh:
        tree = etree.parse(batch_fh)
    n_articles = 0
    for article in tree.iterfind("article"):
        pmcid = _utils.get_pmcid(article)
        bucket = _utils.article_bucket_from_pmcid(pmcid)
        article_dir = output_dir.joinpath(bucket, f"pmcid_{pmcid}")
        article_dir.mkdir(exist_ok=True, parents=True)
        article_file = article_dir.joinpath("article.xml")
        article_file.write_bytes(
            etree.tostring(article, encoding="UTF-8", xml_declaration=True)
        )
        n_articles += 1
    return n_articles


def _extract_tables(article_dir: Path) -> None:
    # a parsed stylesheet (lxml.XSLT) cannot be pickled so we parse it here
    # rather than outside the joblib.Parallel call. Parsing is cached.
    stylesheet = _utils.load_stylesheet("table_extraction.xsl")
    try:
        # We re-parse the article to make sure it is a standalone document to
        # avoid XSLT errors.
        tables_xml = stylesheet(
            etree.parse(str(article_dir.joinpath("article.xml")))
        )
        # remove the DTD added by docbook
        tables_xml.docinfo.clear()
    except Exception:
        _LOG.exception(f"failed to transform article: {stylesheet.error_log}")
        return
    tables_dir = article_dir.joinpath("tables")
    tables_dir.mkdir(exist_ok=True, parents=True)
    tables_file = tables_dir.joinpath("tables.xml")
    tables_file.write_bytes(
        etree.tostring(tables_xml, encoding="UTF-8", xml_declaration=True)
    )
    _extract_tables_content(tables_xml, tables_dir)


def _extract_tables_content(
    tables_xml: etree.Element, tables_dir: Path
) -> None:
    for table_nb, table in enumerate(tables_xml.iterfind("extracted-table")):
        try:
            table_info = {}
            table_info["table_id"] = table.find("table-id").text
            table_info["table_label"] = table.find("table-label").text
            table_info["table_caption"] = table.find("table-caption").text
            kwargs = {}
            if not table.xpath("(.//th|.//thead)"):
                kwargs["header"] = 0
            table_data = pd.read_html(
                etree.tostring(
                    table.find("transformed-table//{*}table")
                ).decode("utf-8"),
                thousands=None,
                flavor="lxml",
                **kwargs,
            )[0]
            table_info["n_header_rows"] = table_data.columns.nlevels
        except Exception:
            # tables may fail to be parsed for various reasons eg they can be
            # empty.
            pass
        else:
            table_name = f"table_{table_nb:0>3}"
            table_data_file = f"{table_name}.csv"
            table_info["table_data_file"] = table_data_file
            table_data.to_csv(
                tables_dir.joinpath(table_data_file), index=False
            )
            tables_dir.joinpath(f"{table_name}_info.json").write_text(
                json.dumps(table_info), "UTF-8"
            )


class ArticleExtractionStep(PipelineStep):
    """Article extraction as part of a pipeline (pubget run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        _utils.add_n_jobs_argument(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, ExitCode]:
        download_dir = previous_steps_output["download"]
        return extract_articles(download_dir, n_jobs=args.n_jobs)


class ArticleExtractionCommand(Command):
    """Article extraction as a standalone command (pubget extract_articles)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "articlesets_dir",
            help="Directory from which to extract articles. It is a directory "
            "created by pubget whose name ends with '_articlesets'. "
            "A sibling directory will be "
            "created to contain the individual article files.",
        )
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = (
            "Extract articles from batches (articleset XML files) "
            "downloaded from PubMed Central by pubget."
        )

    def run(
        self,
        args: argparse.Namespace,
    ) -> ExitCode:
        download_dir = Path(args.articlesets_dir)
        return extract_articles(download_dir, n_jobs=args.n_jobs)[1]
