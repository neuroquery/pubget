"""
.. include:: ../../README.md
"""

from nqdc._utils import configure_logging as _configure_logging
from nqdc._download import download_articles_for_query
from nqdc._articles import extract_articles
from nqdc._data_extraction import extract_data_to_csv
from nqdc._vectorization import vectorize_corpus_to_npz

_configure_logging()

__all__ = [
    "download_articles_for_query",
    "extract_articles",
    "extract_data_to_csv",
    "vectorize_corpus_to_npz",
]
