from pathlib import Path
import functools
import multiprocessing
import multiprocessing.synchronize
import logging
import argparse
from contextlib import ExitStack
from typing import (
    Generator,
    Dict,
    Optional,
    Tuple,
    Any,
    List,
    Mapping,
    Sequence,
)

from lxml import etree
import pandas as pd

from nqdc._authors import AuthorsExtractor
from nqdc._coordinates import CoordinateExtractor
from nqdc._coordinate_space import CoordinateSpaceExtractor
from nqdc._metadata import MetadataExtractor
from nqdc._text import TextExtractor
from nqdc._writers import CSVWriter
from nqdc._typing import (
    PathLikeOrStr,
    BaseExtractor,
    BaseWriter,
    BaseProcessingStep,
)
from nqdc import _utils


_CHUNK_SIZE = 100
_LOG = logging.getLogger(__name__)


def config_worker_logging() -> None:
    # Silence logging from workers. We could add a QueueHandler or a handler
    # that logs to a file with the pid in its name but it's probably not needed
    # as the extraction doesn't produce much logging output and we can see it
    # whenever n_jobs == 1.
    logging.getLogger("").handlers.clear()


def _extract_data(
    articles_dir: Path,
    n_jobs: int,
    articles_semaphore: multiprocessing.synchronize.Semaphore,
) -> Generator[Optional[Dict[str, Any]], None, None]:
    """Extract data from all articles in articles_dir.

    Yields `None` for articles that cannot be parsed. `articles_semaphore` is
    used to block this if too many articles are waiting to be written.
    """
    data_extractors: List[BaseExtractor] = [
        MetadataExtractor(),
        AuthorsExtractor(),
        TextExtractor(),
        CoordinateExtractor(),
        CoordinateSpaceExtractor(),
    ]
    extract = functools.partial(
        _extract_article_data, data_extractors=data_extractors
    )
    article_files = _iter_article_files(articles_dir, articles_semaphore)
    if n_jobs == 1:
        yield from map(extract, article_files)
    else:
        pool = multiprocessing.Pool(n_jobs, initializer=config_worker_logging)
        try:
            yield from pool.imap_unordered(
                extract,
                article_files,
                chunksize=_CHUNK_SIZE,
            )
        finally:
            # if we use the context manager instead it can cause pytest-cov to
            # hang as it uses terminate() rather than close() see
            # https://pytest-cov.readthedocs.io/en/latest/subprocess-support.html
            pool.close()
            pool.join()


def _extract_article_data(
    article_file: Path, data_extractors: Sequence[BaseExtractor]
) -> Optional[Dict[str, Any]]:
    """Extract data from one article. Returns `None` if parsing fails."""
    try:
        article = etree.parse(str(article_file))
    except Exception:
        _LOG.exception(f"Failed to parse {article_file}")
        return None
    article_data = {}
    for extractor in data_extractors:
        try:
            article_data[extractor.name] = extractor.extract(article)
        except Exception:
            _LOG.exception(
                f"Extractor '{extractor.name}' failed on {article_file}."
            )
    return article_data


def _iter_article_files(
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
            for article_file in subdir.glob("pmcid_*.xml"):
                # Throttle processing articles so they don't accumulate in the
                # Pool's output queue. When joblib.Parallel starts returning
                # iterators we can use it instead of Pool
                articles_semaphore.acquire()
                yield article_file


def _get_output_dir(
    articles_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr],
    articles_with_coords_only: bool,
) -> Path:
    """Choose an appropriate output directory & create if necessary."""
    if output_dir is None:
        articles_dir = Path(articles_dir)
        subset_name = (
            "articlesWithCoords"
            if articles_with_coords_only
            else "allArticles"
        )
        output_dir = articles_dir.with_name(
            f"subset_{subset_name}_extractedData"
        )
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    return output_dir


def extract_data_to_csv(
    articles_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    *,
    articles_with_coords_only: bool = False,
    n_jobs: int = 1,
) -> Tuple[Path, int]:
    """Extract text and coordinates from articles and store in csv files.

    Parameters
    ----------
    articles_dir
        Directory containing the article files. It is a directory created by
        `nqdc.extract_articles`: it is named `articles` and contains
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
        0 if previous (article extraction) step was complete and this step
        (data extraction) finished normally as well. Used by the `nqdc`
        command-line interface.
    """
    articles_dir = Path(articles_dir)
    _utils.assert_exists(articles_dir)
    output_dir = _get_output_dir(
        articles_dir, output_dir, articles_with_coords_only
    )
    status = _utils.check_steps_status(articles_dir, output_dir, __name__)
    if not status["need_run"]:
        return output_dir, 0
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
        name="data_extraction",
        is_complete=is_complete,
        n_articles=n_articles,
    )
    _LOG.info(f"Done extracting article data to csv files in {output_dir}")
    return output_dir, int(not is_complete)


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
    all_writers: List[BaseWriter] = [
        CSVWriter.from_extractor(MetadataExtractor, output_dir),
        CSVWriter.from_extractor(AuthorsExtractor, output_dir),
        CSVWriter.from_extractor(TextExtractor, output_dir),
        CSVWriter.from_extractor(CoordinateExtractor, output_dir),
        CSVWriter.from_extractor(CoordinateSpaceExtractor, output_dir),
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
            articles_dir, n_jobs=n_jobs, articles_semaphore=articles_semaphore
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
    argument_parser: argparse.ArgumentParser,
) -> None:
    argument_parser.add_argument(
        "--articles_with_coords_only",
        action="store_true",
        help="Only keep data for articles in which stereotactic coordinates "
        "are found.",
    )
    _utils.add_n_jobs_argument(argument_parser)


class DataExtractionStep(BaseProcessingStep):
    name = "data_extraction"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        _edit_argument_parser(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return extract_data_to_csv(
            previous_steps_output["article_extraction"],
            articles_with_coords_only=args.articles_with_coords_only,
            n_jobs=args.n_jobs,
        )


class StandaloneDataExtractionStep(BaseProcessingStep):
    name = "data_extraction"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        argument_parser.add_argument(
            "articles_dir",
            help="Directory containing articles "
            "from which text and coordinates will be extracted. It is a "
            "directory created by the nqdc_extract_articles command. "
            "A sibling directory will be created to contain "
            "the extracted data",
        )
        _edit_argument_parser(argument_parser)
        argument_parser.description = (
            "Extract text, metadata and coordinates from articles."
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return extract_data_to_csv(
            args.articles_dir,
            articles_with_coords_only=args.articles_with_coords_only,
            n_jobs=args.n_jobs,
        )
