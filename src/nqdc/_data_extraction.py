from pathlib import Path
import logging
import json
from contextlib import ExitStack
from typing import Generator, Dict, Optional, Tuple, Any, List

from lxml import etree

from nqdc._authors import AuthorsExtractor
from nqdc._coordinates import CoordinateExtractor
from nqdc._metadata import MetadataExtractor
from nqdc._text import TextExtractor
from nqdc._writers import CSVWriter
from nqdc._typing import PathLikeOrStr, BaseExtractor, BaseWriter
from nqdc import _utils


_LOG = logging.getLogger(__name__)


def extract_data(
    articles_dir: PathLikeOrStr, *, articles_with_coords_only: bool = True
) -> Generator[Dict[str, Any], None, None]:
    """Extract text and coordinates from articles.

    Parameters
    ----------
    articles_dir
        Directory containing the article files. It is a directory created by
        `nqdc.extract_articles`: it is named `articles` and contains
        subdirectories `000` - `fff`, each of which contains articles stored in
        XML files.
    articles_with_coords_only
        If true, articles that contain no stereotactic coordinates are ignored.

    Yields
    ------
    article_data
        Data extracted from one article. Keys are:
        - metadata: a dictionary containing metadata such as pmcid and doi.
        - authors: a `pd.DataFrame` with columns `pmcid`, `surname`,
          `given-names`.
        - text: a dictionary mapping parts such as "abstract" to their content.
        - coordinates: a `pd.DataFrame` containing the extracted coordinates.
    """
    articles_dir = Path(articles_dir)
    _utils.assert_exists(articles_dir)
    data_extractors: List[BaseExtractor] = [
        MetadataExtractor(),
        AuthorsExtractor(),
        TextExtractor(),
        CoordinateExtractor(),
    ]
    for article, article_file in iter_articles(articles_dir):
        article_info = {}
        for extractor in data_extractors:
            try:
                article_info[extractor.name] = extractor.extract(article)
            except Exception:
                _LOG.exception(
                    f"Extractor '{extractor.name}' failed on {article_file}."
                )
        yield article_info


def iter_articles(
    articles_dir: PathLikeOrStr,
) -> Generator[Tuple[etree.ElementTree, Path], None, None]:
    """Generator that iterates over all articles in a directory.

    Articles are parsed and provided as ElementTrees. Articles that fail to be
    parsed are skipped. The order in which articles are visited is
    always the same.

    Parameters
    ----------
    articles_dir
        Directory containing the article files. It is a directory created by
        `nqdc.extract_articles`: it is named `articles` and contains
        subdirectories `000` - `fff`, each of which contains articles stored in
        XML files.

    Yields
    ------
    article
        A parsed article.
    article_file
        File from which the article was parsed.
    """
    articles_dir = Path(articles_dir)
    _utils.assert_exists(articles_dir)
    n_articles = 0
    for subdir in sorted([f for f in articles_dir.glob("*") if f.is_dir()]):
        for article_file in sorted(subdir.glob("pmcid_*.xml")):
            try:
                article = etree.parse(str(article_file))
            except Exception:
                _LOG.exception(f"Failed to parse {article_file}")
            else:
                yield article, article_file
            finally:
                n_articles += 1
                if not n_articles % 20:
                    _LOG.info(
                        f"In directory {subdir.name}: "
                        f"processed {n_articles} articles"
                    )


def _get_output_dir(
    articles_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr],
    articles_with_coords_only: bool,
) -> Path:
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
    _LOG.info(
        f"Extracting data from articles in {articles_dir} to {output_dir}"
    )
    if _utils.is_step_complete(output_dir, "data_extraction"):
        _LOG.info("Data extraction complete, nothing to do.")
        return output_dir, 0
    article_extraction_complete = _utils.is_step_complete(
        articles_dir, "article_extraction"
    )
    if not article_extraction_complete:
        _LOG.warning(
            "Not all articles have been extracted from download dir "
            "or download is incomplete."
        )
    n_articles = _do_extract_data_to_csv(
        articles_dir, output_dir, articles_with_coords_only
    )
    info = {
        "n_articles": n_articles,
        "data_extraction_complete": article_extraction_complete,
    }
    output_dir.joinpath("info.json").write_text(
        json.dumps(info),
        "utf-8",
    )
    _LOG.info(f"Done extracting article data to csv files in {output_dir}")
    return output_dir, int(not article_extraction_complete)


def _do_extract_data_to_csv(
    articles_dir: Path, output_dir: Path, articles_with_coords_only: bool
) -> int:
    all_writers: List[BaseWriter] = [
        CSVWriter.from_extractor(MetadataExtractor, output_dir),
        CSVWriter.from_extractor(AuthorsExtractor, output_dir),
        CSVWriter.from_extractor(TextExtractor, output_dir),
        CSVWriter.from_extractor(CoordinateExtractor, output_dir),
    ]
    with ExitStack() as stack:
        for writer in all_writers:
            stack.enter_context(writer)
        n_articles = 0
        for article_data in extract_data(
            articles_dir, articles_with_coords_only=articles_with_coords_only
        ):
            n_articles += 1
            for writer in all_writers:
                writer.write(article_data)
    return n_articles
