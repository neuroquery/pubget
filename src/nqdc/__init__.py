"""
.. include:: ../../README.md
"""
from nqdc import _utils
from nqdc._download import download_articles_for_query
from nqdc._articles import extract_articles
from nqdc._data_extraction import extract_data_to_csv
from nqdc._vocabulary import extract_vocabulary_to_csv
from nqdc._vectorization import vectorize_corpus_to_npz
from nqdc._fit_neuroquery import fit_neuroquery
from nqdc._fit_neurosynth import fit_neurosynth
from nqdc._nimare import make_nimare_dataset
from nqdc._labelbuddy import make_labelbuddy_documents
from nqdc._typing import BaseProcessingStep

__version__ = _utils.get_nqdc_version()

__all__ = [
    "download_articles_for_query",
    "extract_articles",
    "extract_data_to_csv",
    "extract_vocabulary_to_csv",
    "vectorize_corpus_to_npz",
    "fit_neuroquery",
    "fit_neurosynth",
    "make_nimare_dataset",
    "make_labelbuddy_documents",
    "BaseProcessingStep",
]
