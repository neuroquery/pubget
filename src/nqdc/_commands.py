import argparse
from pathlib import Path
import re

from nqdc._download import download_articles_for_search_term
from nqdc._articles import extract_articles
from nqdc._data_extraction import extract_to_csv
from nqdc._bow_features import vectorize_corpus_to_npz


def download():
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("search_term")
    parser.add_argument("-n", "--n_docs", type=int, default=None)
    parser.add_argument("--email", type=str, default=None)
    args = parser.parse_args()
    download_dir = download_articles_for_search_term(
        term=args.search_term,
        data_dir=args.data_dir,
        n_docs=args.n_docs,
        email=args.email,
    )
    articles_dir = f"{download_dir}-articles"
    extract_articles(download_dir, articles_dir)


def extract():
    parser = argparse.ArgumentParser()
    parser.add_argument("articles_dir")
    parser.add_argument("--articles_with_coords_only", action="store_true")
    parser.add_argument("-o", "--output_dir", type=str, default=None)
    args = parser.parse_args()
    articles_dir = Path(args.articles_dir)
    if args.output_dir is None:
        match = re.match(r"(query-[a-zA-Z0-9]+)-articles", articles_dir.name)
        if match:
            name = f"{match.group(1)}-extracted_data"
        else:
            name = f"{articles_dir.name}-extracted_data"
        output_dir = articles_dir.parent.joinpath(name)
    else:
        output_dir = args.output_dir
    extract_to_csv(articles_dir, output_dir, args.articles_with_coords_only)


def vectorize():
    parser = argparse.ArgumentParser()
    parser.add_argument("extracted_data_dir")
    parser.add_argument("vocabulary_file")
    parser.add_argument("-o", "--output_dir", type=str, default=None)
    args = parser.parse_args()
    data_dir = Path(args.extracted_data_dir)
    if args.output_dir is None:
        match = re.match(r"(query-[a-zA-Z0-9]+)-extracted_data", data_dir.name)
        if match:
            name = f"{match.group(1)}-vectorized_text"
        else:
            name = f"{data_dir.name}-extracted_data"
        output_dir = data_dir.parent.joinpath(name)
    else:
        output_dir = args.output_dir
    vectorize_corpus_to_npz(
        data_dir.joinpath("text.csv"), args.vocabulary_file, output_dir
    )
