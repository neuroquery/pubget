"""'extract_data' step: extract metadata, text and coordinates from XML."""
import argparse
import functools
import logging
import multiprocessing
import multiprocessing.synchronize
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Dict, Generator, Mapping, Optional, Sequence, Tuple

import pandas as pd
from lxml import etree

from pubget import _utils
from pubget._authors import AuthorsExtractor
from pubget._coordinate_space import CoordinateSpaceExtractor
from pubget._coordinates import CoordinateExtractor
from pubget._links import LinkExtractor
from pubget._metadata import MetadataExtractor
from pubget._text import TextExtractor
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    Extractor,
    PathLikeOrStr,
    PipelineStep,
    StopPipeline,
)
from pubget._writers import CSVWriter

_LOG = logging.getLogger(__name__)
_STEP_NAME = "extract_data"
_STEP_DESCRIPTION = "Extract metadata, text and coordinates from articles."
_CHUNK_SIZE = 100


def _config_worker_logging() -> None:
    # Silence logging from workers. We could add a QueueHandler or a handler
    # that logs to a file with the pid in its name but it's probably not needed
    # as the extraction doesn't produce much logging output and we can see it
    # whenever n_jobs == 1.
    logging.getLogger("").handlers.clear()


def _extract_data(
    articles_dir: Path,
    data_extractors: Sequence[Extractor],
    n_jobs: int,
    articles_semaphore: multiprocessing.synchronize.Semaphore,
) -> Generator[Optional[Dict[str, Any]], None, None]:
    """Extract data from all articles in articles_dir.

    Yields `None` for articles that cannot be parsed. `articles_semaphore` is
    used to block this if too many articles are waiting to be written.
    """
    extract = functools.partial(
        _extract_article_data, data_extractors=data_extractors
    )
    articles = _iter_articles(articles_dir, articles_semaphore)
    if n_jobs == 1:
        yield from map(extract, articles)
    else:
        # if we use the context manager it can cause pytest-cov to hang as
        # __exit__ uses terminate() rather than close(); see
        # https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
        # so we use the try/finally block and call close() and join() instead

        # pylint: disable-next=consider-using-with
        pool = multiprocessing.Pool(n_jobs, initializer=_config_worker_logging)
        try:
            yield from pool.imap_unordered(
                extract,
                articles,
                chunksize=_CHUNK_SIZE,
            )
        finally:
            pool.close()
            pool.join()


def _extract_article_data(
    article_dir: Path, data_extractors: Sequence[Extractor]
) -> Optional[Dict[str, Any]]:
    """Extract data from one article. Returns `None` if parsing fails."""
    article_file = article_dir.joinpath("article.xml")
    try:
        article = etree.parse(str(article_file))
    except Exception:
        _LOG.exception(f"Failed to parse {article_file}")
        return None
    article_data = {}
    for extractor in data_extractors:
        try:
            article_data[extractor.name] = extractor.extract(
                article, article_dir
            )
        except Exception:
            _LOG.exception(
                f"Extractor '{extractor.name}' failed on {article_file}."
            )
    return article_data


def _iter_articles(
    articles_dir: Path,
    articles_semaphore: multiprocessing.synchronize.Semaphore,
) -> Generator[Path, None, None]:
    """Iterate over the paths of all article files in `articles_dir`.

    Acquires `articles_semaphore` before yielding each article so we can avoid
    reading too many before writing the extracted data to the csv files in case
    we are using many workers.
    """
    articles_dir = Path(articles_dir)
    for subdir in articles_dir.glob("*"):
        if subdir.is_dir():
            for article_dir in subdir.glob("pmcid_*"):
                # Throttle processing articles so they don't accumulate in the
                # Pool's output queue. When joblib.Parallel starts returning
                # iterators we can use it instead of Pool
                articles_semaphore.acquire()
                yield article_dir


def extract_data_to_csv(
    articles_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    *,
    articles_with_coords_only: bool = False,
    n_jobs: int = 1,
) -> Tuple[Path, ExitCode]:
    """Extract text and coordinates from articles and store in csv files.

    Parameters
    ----------
    articles_dir
        Directory containing the article files. It is a directory created by
        `pubget.extract_articles`: it is named `articles` and contains
        subdirectories `000` - `fff`, each of which contains articles stored in
        XML files.
    output_dir
        Directory in which to store the extracted data. If not specified, a
        sibling directory of `articles_dir` is used. Its name is
        `subset_allArticles_extractedData` or
        `subset_articlesWithCoords_extractedData`, depending on the value of
        `articles_with_coords_only`.
    articles_with_coords_only
        If True, articles that contain no stereotactic coordinates are ignored.
    n_jobs
        Number of processes to run in parallel. `-1` means using all
        processors.

    Returns
    -------
    output_dir
        The directory in which extracted data is stored.
    exit_code
        COMPLETED if previous (article extraction) step was complete and this
        step (data extraction) finished normally as well. Used by the `pubget`
        command-line interface.
    """
    articles_dir = Path(articles_dir)
    _utils.assert_exists(articles_dir)
    subset_name = (
        "articlesWithCoords" if articles_with_coords_only else "allArticles"
    )
    output_dir = _utils.get_output_dir(
        articles_dir,
        output_dir,
        "articles",
        f"subset_{subset_name}_extractedData",
    )
    status = _utils.check_steps_status(articles_dir, output_dir, __name__)
    if not status["need_run"]:
        return output_dir, ExitCode.COMPLETED
    _LOG.info(
        f"Extracting data from articles in {articles_dir} to {output_dir}"
    )
    n_jobs = _utils.check_n_jobs(n_jobs)
    n_articles = _do_extract_data_to_csv(
        articles_dir, output_dir, articles_with_coords_only, n_jobs=n_jobs
    )
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(
        output_dir,
        name=_STEP_NAME,
        is_complete=is_complete,
        n_articles=n_articles,
    )
    _LOG.info(f"Done extracting article data to csv files in {output_dir}")
    exit_code = ExitCode.COMPLETED if is_complete else ExitCode.INCOMPLETE
    return output_dir, exit_code


def _do_extract_data_to_csv(
    articles_dir: Path,
    output_dir: Path,
    articles_with_coords_only: bool,
    n_jobs: int,
) -> int:
    """Do the data extraction and return the number of articles whose data was
    saved. If `articles_with_coords_only` only articles with at least one
    sterotactic coordinate triplet have their data saved.
    """
    n_to_process = _utils.get_n_articles(articles_dir)
    data_extractors = [
        MetadataExtractor(),
        AuthorsExtractor(),
        TextExtractor(),
        CoordinateExtractor(),
        CoordinateSpaceExtractor(),
        LinkExtractor(),
    ]
    all_writers = [
        CSVWriter.from_extractor(extractor, output_dir)
        for extractor in data_extractors
    ]
    with ExitStack() as stack:
        for writer in all_writers:
            stack.enter_context(writer)
        n_processed_articles = 0
        n_kept_articles = 0
        # Slows down reading & processing articles if we don't write them fast
        # enough.
        articles_semaphore = multiprocessing.Semaphore(_CHUNK_SIZE * n_jobs)
        for article_data in _extract_data(
            articles_dir,
            data_extractors,
            n_jobs=n_jobs,
            articles_semaphore=articles_semaphore,
        ):
            if _should_write(article_data, articles_with_coords_only):
                assert article_data is not None  # for mypy
                for writer in all_writers:
                    writer.write(article_data)
                n_kept_articles += 1
            articles_semaphore.release()
            n_processed_articles += 1
            _report_progress(n_processed_articles, n_to_process)
    return n_kept_articles


def _should_write(
    article_data: Optional[Dict[str, Any]], articles_with_coords_only: bool
) -> bool:
    """Whether an article's data needs to be saved."""
    if article_data is None:
        return False
    if not articles_with_coords_only:
        return True
    if "coordinates" not in article_data:
        return False
    coord: pd.DataFrame = article_data["coordinates"]
    if coord.shape[0]:
        return True
    return False


def _report_progress(n_articles: int, n_to_process: Optional[int]) -> None:
    if not n_articles % _CHUNK_SIZE or n_articles == n_to_process:
        n_msg = f" / {n_to_process}" if n_to_process is not None else ""
        _LOG.info(f"Processed {n_articles}{n_msg} articles.")


def _edit_argument_parser(
    argument_parser: ArgparseActions,
) -> None:
    argument_parser.add_argument(
        "--articles_with_coords_only",
        action="store_true",
        help="Only keep data for articles in which stereotactic coordinates "
        "are found.",
    )
    _utils.add_n_jobs_argument(argument_parser)


class DataExtractionStep(PipelineStep):
    """Data extraction as part of a pipeline (pubget run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        _edit_argument_parser(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, ExitCode]:
        output_dir, exit_code = extract_data_to_csv(
            previous_steps_output["extract_articles"],
            articles_with_coords_only=args.articles_with_coords_only,
            n_jobs=args.n_jobs,
        )
        if not _utils.get_n_articles(output_dir):
            raise StopPipeline(
                "No articles matching the query and selection criteria "
                "could be extracted."
            )
        return output_dir, exit_code


class DataExtractionCommand(Command):
    """Data extraction as a standalone command (pubget extract_data)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "articles_dir",
            help="Directory containing articles "
            "from which text and coordinates will be extracted. It is a "
            "directory created by pubget whose name ends with '_articles'. "
            "A sibling directory will be created to contain "
            "the extracted data.",
        )
        _edit_argument_parser(argument_parser)
        argument_parser.description = (
            "Extract text, metadata and coordinates from articles."
        )

    def run(self, args: argparse.Namespace) -> ExitCode:
        return extract_data_to_csv(
            args.articles_dir,
            articles_with_coords_only=args.articles_with_coords_only,
            n_jobs=args.n_jobs,
        )[1]
