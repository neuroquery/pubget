import argparse
from pathlib import Path
import os
from typing import Optional, List, Dict

from nqdc._utils import add_log_file
from nqdc._download import download_articles_for_query
from nqdc._articles import extract_articles
from nqdc._data_extraction import extract_data_to_csv
from nqdc._vectorization import vectorize_corpus_to_npz


def _add_log_file_if_possible(args: argparse.Namespace, prefix: str) -> None:
    log_dir = args.log_dir
    if log_dir is None:
        log_dir = os.environ.get("NQDC_LOG_DIR", None)
    if log_dir is None:
        return
    add_log_file(log_dir, prefix)


def _get_api_key(args: argparse.Namespace) -> Optional[str]:
    if args.api_key is not None:
        return str(args.api_key)
    return os.environ.get("NQDC_API_KEY", None)


def _get_query(args: argparse.Namespace) -> str:
    if args.query is not None:
        return str(args.query)
    return Path(args.query_file).read_text("utf-8")


def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log_dir",
        type=str,
        default=None,
        help="Directory in which to store log files. Can also be specified by "
        "exporting the NQDC_LOG_DIR environment variable (if both are given "
        "the command-line argument has higher precedence). If not specified, "
        "no log file is written.",
    )
    return parser


def _voc_kwarg(voc_file: Optional[str]) -> Dict[str, str]:
    if voc_file is None:
        return {}
    return {"vocabulary": voc_file}


def _get_download_parser() -> argparse.ArgumentParser:
    parser = _get_parser()
    parser.description = (
        "Download full-text articles from PubMed Central for the given query."
    )
    parser.add_argument(
        "data_dir",
        help="Directory in which all nqdc data should be stored. "
        "A subdirectory will be created for the given query.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-q",
        "--query",
        type=str,
        default=None,
        help="Query with which to search the PubMed Central database. "
        "The query can alternatively be read from a file by using the "
        "query_file parameter.",
    )
    group.add_argument(
        "-f",
        "--query_file",
        type=str,
        default=None,
        help="File in which the query is stored. The query can alternatively "
        "be provided as a string by using the query parameter.",
    )
    parser.add_argument(
        "-n",
        "--n_docs",
        type=int,
        default=None,
        help="Approximate maximum number of articles to download. By default, "
        "all results returned for the search are downloaded. If n_docs is "
        "specified, at most n_docs rounded up to the nearest multiple of 500 "
        "articles will be downloaded.",
    )
    parser.add_argument(
        "--api_key",
        type=str,
        default=None,
        help="API key for the Entrez E-utilities (see "
        "https://www.ncbi.nlm.nih.gov/books/NBK25497/). Can also be provided "
        "by exporting the NQDC_API_KEY environment variable (if both are "
        "specified the command-line argument has higher precedence). If the "
        "API key is provided, it is included in all requests to the Entrez "
        "E-utilities.",
    )
    return parser


def download_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_download_parser()
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "download_")
    api_key = _get_api_key(args)
    query = _get_query(args)
    _, code = download_articles_for_query(
        query=query,
        data_dir=args.data_dir,
        n_docs=args.n_docs,
        api_key=api_key,
    )
    return code


def _get_extract_articles_parser() -> argparse.ArgumentParser:
    parser = _get_parser()
    parser.description = (
        "Extract articles from batches (articleset XML files) "
        "downloaded from PubMed Central by the nqdc_download command."
    )
    parser.add_argument(
        "articlesets_dir",
        help="Directory from which to extract articles. It is a directory "
        "created by the nqdc_download command. A sibling directory will be "
        "created to contain the individual article files",
    )
    return parser


def extract_articles_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_extract_articles_parser()
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "extract_articles_")
    download_dir = Path(args.articlesets_dir)
    _, code = extract_articles(download_dir)
    return code


def _add_coords_only_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--articles_with_coords_only",
        action="store_true",
        help="Only keep data for articles in which stereotactic coordinates "
        "are found.",
    )


def _get_extract_data_parser() -> argparse.ArgumentParser:
    parser = _get_parser()
    parser.description = (
        "Extract text, metadata and coordinates from articles."
    )
    parser.add_argument(
        "articles_dir",
        help="Directory containing articles "
        "from which text and coordinates will be extracted. It is a "
        "directory created by the nqdc_extract_articles command. A sibling "
        "directory will be created to contain the extracted data",
    )
    _add_coords_only_arg(parser)
    return parser


def extract_data_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_extract_data_parser()
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "extract_data_")
    _, code = extract_data_to_csv(
        args.articles_dir,
        articles_with_coords_only=args.articles_with_coords_only,
    )
    return code


def _add_voc_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-v",
        "--vocabulary_file",
        type=str,
        default=None,
        help="Vocabulary used to vectorize the text: each dimension of the "
        "vectorized text corresponds to a term in this vocabulary. If not "
        "provided, the default vocabulary used by the neuroquery "
        "package (https://github.com/neuroquery/neuroquery) is used.",
    )


def _get_vectorize_parser() -> argparse.ArgumentParser:
    parser = _get_parser()
    parser.description = (
        "Vectorize text by computing word counts and "
        "TFIDF features. The text comes from csv files created by "
        "the nqdc_extract_data command."
    )
    parser.add_argument(
        "extracted_data_dir",
        help="Directory containing the csv file text.csv created by "
        "the nqdc_extract_data command. A sibling directory will be "
        "created for the vectorized data",
    )
    _add_voc_arg(parser)
    return parser


def vectorize_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_vectorize_parser()
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "vectorize_")
    data_dir = Path(args.extracted_data_dir)
    _, code = vectorize_corpus_to_npz(
        data_dir, **_voc_kwarg(args.vocabulary_file)
    )
    return code


def _get_full_pipeline_parser() -> argparse.ArgumentParser:
    parser = _get_download_parser()
    parser.description = (
        "Download and process full-text articles from PubMed Central "
        "for the given query. Articles are downloaded and stored in "
        "individual files. Then, their text and stereotactic coordinates "
        "are extracted and stored in csv files. Finally, the text is "
        "vectorized by computing word counts and TFIDF features."
    )
    _add_voc_arg(parser)
    _add_coords_only_arg(parser)
    return parser


def full_pipeline_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_full_pipeline_parser()
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "full_pipeline_")
    api_key = _get_api_key(args)
    query = _get_query(args)
    total_code = 0
    download_dir, code = download_articles_for_query(
        query=query,
        data_dir=args.data_dir,
        n_docs=args.n_docs,
        api_key=api_key,
    )
    total_code += code
    articles_dir, code = extract_articles(download_dir)
    total_code += code
    extracted_data_dir, code = extract_data_to_csv(
        articles_dir,
        articles_with_coords_only=args.articles_with_coords_only,
    )
    total_code += code
    _, code = vectorize_corpus_to_npz(
        extracted_data_dir, **_voc_kwarg(args.vocabulary_file)
    )
    total_code += code
    return total_code
