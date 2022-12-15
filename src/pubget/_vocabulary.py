"""'extract_vocabulary' step: get vocabulary from extracted text."""
import argparse
import logging
from pathlib import Path
from typing import Generator, Mapping, Optional, TextIO, Tuple

import numpy as np
import pandas as pd
from neuroquery import tokenization
from sklearn.feature_extraction.text import CountVectorizer

from pubget import _utils
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    PathLikeOrStr,
    PipelineStep,
)

_LOG = logging.getLogger(__name__)
_STEP_NAME = "extract_vocabulary"
_STEP_DESCRIPTION = "Extract vocabulary of word n-grams from text."


def _iter_corpus(corpus_fh: TextIO) -> Generator[str, None, None]:
    """Yield the concatenated text fields of articles one by one."""
    n_articles = 0
    for chunk in pd.read_csv(corpus_fh, chunksize=500):
        chunk.fillna("", inplace=True)
        text = chunk["title"].str.cat(
            chunk.loc[:, ["keywords", "abstract", "body"]], sep="\n"
        )
        for article_text in text.values:
            yield article_text
        n_articles += chunk.shape[0]
        _LOG.info(f"Processed {n_articles} articles.")


def extract_vocabulary(extracted_data_dir: PathLikeOrStr) -> pd.Series:
    """Extract vocabulary and document frequencies.

    Parameters
    ----------
    extracted_data_dir
        The directory containing the text of articles to vectorize. It is a
        directory created by `pubget.extract_data_to_csv`: it contains a file
        named `text.csv` with fields `pmcid`, `title`, `keywords`, `abstract`,
        `body`.

    Returns
    -------
    doc_freq
        Index are the vocabulary terms and values are their document
        frequencies.
    """
    corpus_file = Path(extracted_data_dir).joinpath("text.csv")
    _utils.assert_exists(corpus_file)
    vectorizer = CountVectorizer(
        input="content",
        lowercase=True,
        stop_words=list(tokenization.nltk_stop_words()),
        tokenizer=tokenization.Tokenizer(),
        ngram_range=(1, 2),
        binary=True,
        dtype=np.float32,
        min_df=0.001,
    )
    with open(corpus_file, encoding="utf-8") as corpus_fh:
        counts = vectorizer.fit_transform(_iter_corpus(corpus_fh))
    n_docs = counts.shape[0]
    doc_counts = np.asarray(counts.sum(axis=0)).squeeze()
    doc_freq = pd.Series(
        (doc_counts + 1) / (n_docs + 1), index=vectorizer.vocabulary_
    )
    doc_freq.sort_index(inplace=True)
    return doc_freq


def extract_vocabulary_to_csv(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
) -> Tuple[Path, ExitCode]:
    """Extract vocabulary and document frequencies and write to csv.

    Parameters
    ----------
    extracted_data_dir
        The directory containing the text of articles to vectorize. It is a
        directory created by `pubget.extract_data_to_csv`: it contains a file
        named `text.csv` with fields `pmcid`, `title`, `keywords`, `abstract`,
        `body`.

    Returns
    -------
    output_dir
        The directory in which the vocabulary is stored.
    exit_code
        COMPLETED if previous (data extraction) step was complete and this step
        (vocabulary extraction) finished normally as well. Used by the `pubget`
        command-line interface.
    """
    extracted_data_dir = Path(extracted_data_dir)
    output_dir = _utils.get_output_dir(
        extracted_data_dir,
        output_dir,
        "_extractedData",
        "_extractedVocabulary",
    )
    status = _utils.check_steps_status(
        extracted_data_dir, output_dir, __name__
    )
    if not status["need_run"]:
        return output_dir, ExitCode.COMPLETED
    _LOG.info(f"Extracting vocabulary from {extracted_data_dir}")
    doc_freq = extract_vocabulary(extracted_data_dir)
    doc_freq.to_csv(output_dir.joinpath("vocabulary.csv"), header=None)
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(output_dir, name=_STEP_NAME, is_complete=is_complete)
    _LOG.info(f"Done extracting vocabulary to {output_dir}")
    exit_code = ExitCode.COMPLETED if is_complete else ExitCode.INCOMPLETE
    return output_dir, exit_code


class VocabularyExtractionStep(PipelineStep):
    """Extracting vocabulary as part of a pipeline (pubget run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "--extract_vocabulary",
            action="store_true",
            help="Extract vocabulary and document frequencies from "
            "downloaded text.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], ExitCode]:
        if not args.extract_vocabulary:
            return None, ExitCode.COMPLETED
        return extract_vocabulary_to_csv(
            previous_steps_output["extract_data"],
        )


class VocabularyExtractionCommand(Command):
    """Extracting voc as a standalone command (pubget extract_vocabulary)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "extracted_data_dir",
            help="Directory containing the csv file text.csv created by "
            "pubget whose name ends with '_extractedData'. A sibling "
            "directory will be created for the vocabulary.",
        )
        argument_parser.description = (
            "Extract vocabulary and document "
            "frequencies from text downloaded by pubget."
        )

    def run(
        self,
        args: argparse.Namespace,
    ) -> ExitCode:
        return extract_vocabulary_to_csv(args.extracted_data_dir)[1]
