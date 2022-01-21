import argparse
from pathlib import Path
import os
import re
from typing import Optional

from nqdc._utils import add_log_file
from nqdc._download import download_articles_for_query
from nqdc._articles import extract_articles
from nqdc._data_extraction import extract_to_csv
from nqdc._bow_features import vectorize_corpus_to_npz, checksum_vocabulary


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
    parser.add_argument("--log_dir", type=str, default=None)
    return parser


def download_command() -> None:
    parser = _get_parser()
    parser.add_argument("data_dir")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-q", "--query", type=str, default=None)
    group.add_argument("-f", "--query_file", type=str, default=None)
    parser.add_argument("-n", "--n_docs", type=int, default=None)
    parser.add_argument("--api_key", type=str, default=None)
    args = parser.parse_args()
    _add_log_file_if_possible(args, "download_")
    api_key = _get_api_key(args)
    query = _get_query(args)
    download_articles_for_query(
        query=query,
        data_dir=args.data_dir,
        n_docs=args.n_docs,
        api_key=api_key,
    )


def extract_articles_command() -> None:
    parser = _get_parser()
    parser.add_argument("articlesets_dir")
    args = parser.parse_args()
    _add_log_file_if_possible(args, "extract_articles_")
    download_dir = Path(args.articlesets_dir)
    articles_dir = download_dir.parent.joinpath("articles")
    extract_articles(download_dir, articles_dir)


def extract_data_command() -> None:
    parser = _get_parser()
    parser.add_argument("articles_dir")
    parser.add_argument("--articles_with_coords_only", action="store_true")
    args = parser.parse_args()
    _add_log_file_if_possible(args, "extract_data_")
    articles_dir = Path(args.articles_dir)
    subset_name = (
        "articlesWithCoords"
        if args.articles_with_coords_only
        else "allArticles"
    )
    output_dir = articles_dir.parent.joinpath(
        f"subset_{subset_name}_extractedData"
    )
    extract_to_csv(articles_dir, output_dir, args.articles_with_coords_only)


def vectorize_command() -> None:
    parser = _get_parser()
    parser.add_argument("extracted_data_dir")
    parser.add_argument("vocabulary_file")
    args = parser.parse_args()
    _add_log_file_if_possible(args, "vectorize_")
    data_dir = Path(args.extracted_data_dir)
    voc_checksum = checksum_vocabulary(args.vocabulary_file)
    output_dir_name = re.sub(
        r"^(.*?)(_extractedData)?$",
        rf"\1-voc_{voc_checksum}_vectorizedText",
        data_dir.name,
    )
    output_dir = Path(args.extracted_data_dir).parent.joinpath(output_dir_name)
    vectorize_corpus_to_npz(
        data_dir.joinpath("text.csv"), args.vocabulary_file, output_dir
    )
