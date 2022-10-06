"""
.. include:: ../../README.md
"""
from pubget import _utils
from pubget._articles import extract_articles
from pubget._data_extraction import extract_data_to_csv
from pubget._download import download_pmcids, download_query_results
from pubget._fit_neuroquery import fit_neuroquery
from pubget._fit_neurosynth import fit_neurosynth
from pubget._labelbuddy import make_labelbuddy_documents
from pubget._nimare import make_nimare_dataset
from pubget._typing import Command, ExitCode, PipelineStep
from pubget._vectorization import vectorize_corpus_to_npz
from pubget._vocabulary import extract_vocabulary_to_csv

__version__ = _utils.get_pubget_version()

__all__ = [
    "Command",
    "ExitCode",
    "PipelineStep",
    "download_pmcids",
    "download_query_results",
    "extract_articles",
    "extract_data_to_csv",
    "extract_vocabulary_to_csv",
    "fit_neuroquery",
    "fit_neurosynth",
    "make_labelbuddy_documents",
    "make_nimare_dataset",
    "vectorize_corpus_to_npz",
]
