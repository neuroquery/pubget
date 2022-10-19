"""'vectorize' step: compute TFIDF from extracted text."""
import argparse
import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from neuroquery.datasets import fetch_neuroquery_model
from neuroquery.tokenization import TextVectorizer
from scipy import sparse
from sklearn.preprocessing import normalize

from pubget import _utils
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    PathLikeOrStr,
    PipelineStep,
)
from pubget._utils import assert_exists, checksum

_LOG = logging.getLogger(__name__)
_STEP_NAME = "vectorize"
_STEP_DESCRIPTION = "Extract TFIDF features from text."
_FIELDS = ("title", "keywords", "abstract", "body")
_OPTIONS_IMPLYING_TFIDF = (
    "vectorize_text",
    "vocabulary_file",
    "fit_neuroquery",
    "fit_neurosynth",
    "nimare",
)


class Vocabulary(Enum):
    """Enumeration of known vocabularies.

    At the moment only contains the vocabulary used by `neuroquery`.
    """

    NEUROQUERY_VOCABULARY = (
        "https://github.com/neuroquery/"
        "neuroquery_data/blob/main/neuroquery_model/vocabulary.csv"
    )


def vectorize_corpus_to_npz(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    vocabulary: Union[
        PathLikeOrStr, Vocabulary
    ] = Vocabulary.NEUROQUERY_VOCABULARY,
    n_jobs: int = 1,
) -> Tuple[Path, ExitCode]:
    """Compute word counts and TFIDF features and store them in `.npz` files.

    Parameters
    ----------
    extracted_data_dir
        The directory containing the text of articles to vectorize. It is a
        directory created by `pubget.extract_data_to_csv`: it contains a file
        named `text.csv` with fields `pmcid`, `title`, `keywords`, `abstract`,
        `body`.
    output_dir
        The directory in which to store the results. If not specified, a
        sibling directory of `extracted_data_dir` will be used. Its name will
        end with `-voc_<md5 checksum of the vocabulary>_vectorizedText`.
    vocabulary
        A file containing the vocabulary used to vectorize text, with one term
        or phrase per line. Each dimension in the output will correspond to the
        frequency of one entry in this vocabulary. By default, the vocabulary
        used by https://neuroquery.org will be downloaded and used.
    n_jobs
        Number of processes to run in parallel. `-1` means using all
        processors.

    Returns
    -------
    output_dir
        The directory in which the vectorized data is stored.
    exit_code
        COMPLETED if previous (data extraction) step was complete and this step
        (vectorization) finished normally as well. Used by the `pubget`
        command-line interface.
    """
    extracted_data_dir = Path(extracted_data_dir)
    assert_exists(extracted_data_dir.joinpath("text.csv"))
    n_jobs = _utils.check_n_jobs(n_jobs)
    vocabulary_file = _resolve_voc(vocabulary)
    voc_checksum = _checksum_vocabulary(vocabulary_file)
    output_dir = _utils.get_output_dir(
        extracted_data_dir,
        output_dir,
        "_extractedData",
        f"-voc_{voc_checksum}_vectorizedText",
    )
    status = _utils.check_steps_status(
        extracted_data_dir, output_dir, __name__
    )
    if not status["need_run"]:
        return output_dir, ExitCode.COMPLETED
    _LOG.info(
        f"vectorizing {extracted_data_dir} using vocabulary "
        f"{vocabulary_file} to {output_dir}"
    )
    n_articles = _do_vectorize_corpus_to_npz(
        extracted_data_dir, output_dir, vocabulary_file, n_jobs=n_jobs
    )
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(
        output_dir,
        name=_STEP_NAME,
        is_complete=is_complete,
        n_articles=n_articles,
    )
    _LOG.info(f"Done creating BOW features .npz files in {output_dir}")
    exit_code = ExitCode.COMPLETED if is_complete else ExitCode.INCOMPLETE
    return output_dir, exit_code


def _do_vectorize_corpus_to_npz(
    extracted_data_dir: Path,
    output_dir: Path,
    vocabulary_file: Path,
    n_jobs: int,
) -> int:
    """Do the extraction and return number of vectorized articles."""
    extraction_result = vectorize_corpus(
        extracted_data_dir, vocabulary_file, n_jobs=n_jobs
    )
    np.savetxt(
        output_dir.joinpath("pmcid.txt"),
        extraction_result["pmcids"],
        fmt="%i",
        encoding="utf-8",
    )
    for feature_kind in "counts", "tfidf":
        for field, vectorized in extraction_result[feature_kind].items():
            output_file = output_dir.joinpath(f"{field}_{feature_kind}.npz")
            sparse.save_npz(str(output_file), vectorized)
    for voc_name in "feature_names", "vocabulary":
        voc_file = output_dir.joinpath(f"{voc_name}.csv")
        extraction_result[f"document_frequencies_{voc_name}"].to_csv(
            voc_file, header=None
        )
    voc_mapping_file = output_dir.joinpath(
        "vocabulary.csv_voc_mapping_identity.json"
    )
    voc_mapping_file.write_text(
        json.dumps(extraction_result["voc_mapping"]), "utf-8"
    )
    return len(extraction_result["pmcids"])


def _vectorize_articles(
    articles: pd.DataFrame, vectorizer: TextVectorizer
) -> Tuple[Sequence[int], Dict[str, sparse.csr_matrix]]:
    """Vectorize one batch of articles.

    Returns the pmcids and the mapping text field: csr matrix of features.
    """
    articles.fillna("", inplace=True)
    vectorized = {}
    for field in _FIELDS:
        vectorized[field] = vectorizer.transform(articles[field].values)
    return articles["pmcid"].values, vectorized


def _extract_word_counts(
    corpus_file: PathLikeOrStr, vocabulary_file: PathLikeOrStr, n_jobs: int
) -> Tuple[Sequence[int], Dict[str, sparse.csr_matrix], TextVectorizer]:
    """Compute word counts for all articles in a csv file.

    returns the pmcids, mapping of text filed: csr matrix, and the vectorizer.
    order of pmcids matches rows in the feature matrices.
    """
    vectorizer = TextVectorizer.from_vocabulary_file(
        str(vocabulary_file), use_idf=False, norm=None, voc_mapping={}
    ).fit()
    chunksize = 200
    with open(corpus_file, encoding="utf-8") as corpus_fh:
        all_chunks = pd.read_csv(corpus_fh, chunksize=chunksize)
        vectorized_chunks = Parallel(n_jobs=n_jobs, verbose=8)(
            delayed(_vectorize_articles)(chunk, vectorizer=vectorizer)
            for chunk in all_chunks
        )
    vectorized_fields = {}
    for field in _FIELDS:
        vectorized_fields[field] = sparse.vstack(
            [chunk[1][field] for chunk in vectorized_chunks],
            format="csr",
            dtype=int,
        )
    pmcids = np.concatenate([chunk[0] for chunk in vectorized_chunks])
    return pmcids, vectorized_fields, vectorizer


def _get_voc_mapping_file(vocabulary_file: PathLikeOrStr) -> Path:
    return Path(f"{vocabulary_file}_voc_mapping_identity.json")


def _checksum_vocabulary(vocabulary_file: PathLikeOrStr) -> str:
    """md5sum of concatenated voc file and voc mapping file contents."""
    voc = Path(vocabulary_file).read_bytes()
    voc_mapping_file = _get_voc_mapping_file(vocabulary_file)
    if voc_mapping_file.is_file():
        voc += voc_mapping_file.read_bytes()
    return checksum(voc)


def _load_voc_mapping(vocabulary_file: PathLikeOrStr) -> Dict[str, str]:
    """Load the voc mapping corresponding to `vocabulary_file` if it exists"""
    voc_mapping_file = _get_voc_mapping_file(vocabulary_file)
    if voc_mapping_file.is_file():
        voc_mapping: Dict[str, str] = json.loads(
            voc_mapping_file.read_text("utf-8")
        )
    else:
        voc_mapping = {}
    return voc_mapping


def _get_neuroquery_vocabulary() -> Path:
    """Load default voc, downloading it if necessary."""
    return Path(fetch_neuroquery_model()).joinpath("vocabulary.csv")


def _resolve_voc(vocabulary: Union[PathLikeOrStr, Vocabulary]) -> Path:
    """Resolve vocabulary to an existing file path."""
    if vocabulary is Vocabulary.NEUROQUERY_VOCABULARY:
        return _get_neuroquery_vocabulary()
    voc = Path(vocabulary)
    assert_exists(voc)
    return voc


def _counts_to_frequencies(
    counts: Mapping[str, sparse.csr_matrix]
) -> Tuple[Dict[str, sparse.csr_matrix], Sequence[float]]:
    """Compute term and document frequencies."""
    term_freq = {
        k: normalize(v, norm="l1", axis=1, copy=True)
        for k, v in counts.items()
    }
    freq_merged = np.sum(list(term_freq.values())) / len(term_freq)
    term_freq["merged"] = freq_merged
    doc_counts = np.asarray((freq_merged > 0).sum(axis=0)).squeeze()
    n_docs = counts["body"].shape[0]
    doc_freq = (doc_counts + 1) / (n_docs + 1)
    return term_freq, doc_freq


def _apply_voc_mapping(
    counts_full_voc: Mapping[str, sparse.csr_matrix],
    term_freq_full_voc: Mapping[str, sparse.csr_matrix],
    voc: Sequence[str],
    voc_mapping: Mapping[str, str],
) -> Tuple[
    Dict[str, sparse.csr_matrix],
    Dict[str, sparse.csr_matrix],
    Sequence[float],
]:
    """Compute term counts & frequencies for reduced voc after voc mapping."""
    voc_map_mat = _voc_mapping_matrix(voc, voc_mapping)
    counts = {k: v.dot(voc_map_mat.T) for k, v in counts_full_voc.items()}
    term_freq = {
        k: v.dot(voc_map_mat.T) for k, v in term_freq_full_voc.items()
    }
    doc_counts = np.asarray((term_freq["merged"] > 0).sum(axis=0)).squeeze()
    n_docs = counts["body"].shape[0]
    doc_freq = (doc_counts + 1) / (n_docs + 1)
    return counts, term_freq, doc_freq


def _compute_tfidf(
    term_freq: Mapping[str, sparse.csr_matrix],
    doc_freq: Sequence[float],
) -> Dict[str, sparse.csr_matrix]:
    idf = -np.log(doc_freq) + 1
    n_terms = len(idf)
    idf_mat = sparse.spdiags(
        idf,
        diags=0,
        m=n_terms,
        n=n_terms,
        format="csr",
    )
    tfidf = {k: v.dot(idf_mat) for k, v in term_freq.items()}
    return tfidf


def _prepare_bow_data(
    counts_full_voc: Mapping[str, sparse.csr_matrix],
    voc: Sequence[str],
    voc_mapping: Mapping[str, str],
) -> Dict[str, Any]:
    """Compute term & doc frequency data from raw counts and vocabulary.

    The counts and tfidf are for the reduced vocabulary (after applying the
    vocabulary mapping).
    """
    term_freq_full_voc, doc_freq_full_voc = _counts_to_frequencies(
        counts_full_voc
    )
    counts, term_freq, doc_freq = _apply_voc_mapping(
        counts_full_voc, term_freq_full_voc, voc, voc_mapping
    )
    tfidf = _compute_tfidf(term_freq, doc_freq)
    return {
        "counts": counts,
        "tfidf": tfidf,
        "document_frequencies_vocabulary": pd.Series(
            doc_freq_full_voc, index=voc
        ),
        "document_frequencies_feature_names": pd.Series(
            doc_freq,
            index=sorted(set(voc).difference(voc_mapping.keys())),
        ),
        "voc_mapping": voc_mapping,
    }


def vectorize_corpus(
    extracted_data_dir: PathLikeOrStr,
    vocabulary: Union[
        PathLikeOrStr, Vocabulary
    ] = Vocabulary.NEUROQUERY_VOCABULARY,
    n_jobs: int = 1,
) -> Dict[str, Any]:
    """Compute word counts and TFIDF features.

    Parameters
    ----------
    extracted_data_dir
        The directory containing the text of articles to vectorize. It is a
        directory created by `pubget.extract_data_to_csv`: it contains a file
        named `text.csv` with fields `pmcid`, `title`, `keywords`, `abstract`,
        `body`.
    vocabulary
        A file containing the vocabulary used to vectorize text, with one term
        or phrase per line. Each dimension in the output will correspond to the
        frequency of one entry in this vocabulary. By default, the vocabulary
        used by https://neuroquery.org will be downloaded and used.
    n_jobs
        Number of processes to run in parallel. `-1` means using all
        processors.

    Returns
    -------
    vectorized_data
        Contains the pmcids of the vectorized articles, the document
        frequencies of the vocabulary, and the word counts and TFIDF for each
        article section and for whole articles as scipy sparse matrices.

    """
    corpus_file = Path(extracted_data_dir).joinpath("text.csv")
    assert_exists(corpus_file)
    n_jobs = _utils.check_n_jobs(n_jobs)
    vocabulary_file = _resolve_voc(vocabulary)
    pmcids, counts_full_voc, vectorizer = _extract_word_counts(
        corpus_file, vocabulary_file, n_jobs=n_jobs
    )
    voc = vectorizer.get_feature_names()
    voc_mapping = _load_voc_mapping(vocabulary_file)
    data = _prepare_bow_data(counts_full_voc, voc, voc_mapping)
    data["pmcids"] = pmcids
    return data


def _voc_mapping_matrix(
    vocabulary: Sequence[str], voc_mapping: Mapping[str, str]
) -> sparse.csr_matrix:
    """Sparse matrix representing voc mapping as operator on feature vectors.

    `M.dot(v)` applies the vocabulary mapping, where M is the voc mapping
    matrix and v is a tfidf (or word count) vector.
    """
    word_to_idx = pd.Series(np.arange(len(vocabulary)), index=vocabulary)
    form = sparse.eye(len(vocabulary), format="lil", dtype=int)
    keep = np.ones(len(vocabulary), dtype=bool)
    for source, target in voc_mapping.items():
        s_idx, t_idx = word_to_idx[source], word_to_idx[target]
        keep[s_idx] = False
        form[t_idx, s_idx] = 1
    form = form[keep, :]
    return form.tocsr()


def _add_voc_arg(argument_parser: ArgparseActions) -> None:
    argument_parser.add_argument(
        "-v",
        "--vocabulary_file",
        type=str,
        default=None,
        help="Vocabulary used to vectorize the text: each dimension of the "
        "vectorized text corresponds to a term in this vocabulary. If not "
        "provided, the default vocabulary used by the neuroquery "
        "package (https://github.com/neuroquery/neuroquery) is used.",
    )


def _voc_kwarg(
    args: argparse.Namespace, previous_steps_output: Mapping[str, Path]
) -> Dict[str, PathLikeOrStr]:
    if args.vocabulary_file is not None:
        return {"vocabulary": args.vocabulary_file}
    if "extract_vocabulary" in previous_steps_output:
        return {
            "vocabulary": previous_steps_output["extract_vocabulary"].joinpath(
                "vocabulary.csv"
            )
        }
    return {}


class VectorizationStep(PipelineStep):
    """Vectorizing text as part of a pipeline (pubget run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        _add_voc_arg(argument_parser)
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.add_argument(
            "--vectorize_text",
            action="store_true",
            help="Vectorize text by computing word counts and TFIDF features.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], ExitCode]:
        if not any(
            getattr(args, option) for option in _OPTIONS_IMPLYING_TFIDF
        ):
            return None, ExitCode.COMPLETED
        return vectorize_corpus_to_npz(
            previous_steps_output["extract_data"],
            n_jobs=args.n_jobs,
            **_voc_kwarg(args, previous_steps_output),
        )


class VectorizationCommand(Command):
    """Vectorizing text as a standalone command (pubget vectorize)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "extracted_data_dir",
            help="Directory containing the csv file text.csv created by "
            "pubget whose name ends with '_extractedData'. A sibling "
            "directory will be created for the vectorized data.",
        )
        _add_voc_arg(argument_parser)
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = (
            "Vectorize text by computing word counts and "
            "TFIDF features. The text comes from csv files created by "
            "pubget."
        )

    def run(
        self,
        args: argparse.Namespace,
    ) -> ExitCode:
        return vectorize_corpus_to_npz(
            args.extracted_data_dir,
            n_jobs=args.n_jobs,
            **_voc_kwarg(args, {}),
        )[1]
