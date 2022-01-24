import argparse
from pathlib import Path
import os
from typing import Optional, List, Dict

from nqdc._utils import add_log_file
from nqdc._download import download_articles_for_query
from nqdc._articles import extract_articles
from nqdc._data_extraction import extract_to_csv
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
    parser.add_argument("--log_dir", type=str, default=None)
    return parser


def _voc_kwarg(voc_file: Optional[str]) -> Dict[str, str]:
    if voc_file is None:
        return {}
    return {"vocabulary": voc_file}


def download_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_parser()
    parser.add_argument("data_dir")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-q", "--query", type=str, default=None)
    group.add_argument("-f", "--query_file", type=str, default=None)
    parser.add_argument("-n", "--n_docs", type=int, default=None)
    parser.add_argument("--api_key", type=str, default=None)
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


def extract_articles_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_parser()
    parser.add_argument("articlesets_dir")
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "extract_articles_")
    download_dir = Path(args.articlesets_dir)
    _, code = extract_articles(download_dir)
    return code


def extract_data_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_parser()
    parser.add_argument("articles_dir")
    parser.add_argument("--articles_with_coords_only", action="store_true")
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "extract_data_")
    _, code = extract_to_csv(
        args.articles_dir,
        articles_with_coords_only=args.articles_with_coords_only,
    )
    return code


def vectorize_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_parser()
    parser.add_argument("extracted_data_dir")
    parser.add_argument("-v", "--vocabulary_file", type=str, default=None)
    args = parser.parse_args(argv)
    _add_log_file_if_possible(args, "vectorize_")
    data_dir = Path(args.extracted_data_dir)
    _, code = vectorize_corpus_to_npz(
        data_dir.joinpath("text.csv"), **_voc_kwarg(args.vocabulary_file)
    )
    return code


def full_pipeline_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_parser()
    parser.add_argument("data_dir")
    parser.add_argument("-v", "--vocabulary_file", type=str, default=None)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-q", "--query", type=str, default=None)
    group.add_argument("-f", "--query_file", type=str, default=None)
    parser.add_argument("-n", "--n_docs", type=int, default=None)
    parser.add_argument("--api_key", type=str, default=None)
    parser.add_argument("--articles_with_coords_only", action="store_true")
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
    extracted_data_dir, code = extract_to_csv(
        articles_dir,
        articles_with_coords_only=args.articles_with_coords_only,
    )
    total_code += code
    _, code = vectorize_corpus_to_npz(
        extracted_data_dir.joinpath("text.csv"),
        **_voc_kwarg(args.vocabulary_file)
    )
    total_code += code
    return total_code
