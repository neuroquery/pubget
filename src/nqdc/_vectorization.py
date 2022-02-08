from pathlib import Path
import argparse
import re
import logging
import json
from enum import Enum
from typing import Tuple, Dict, Any, Sequence, Optional, Union, Mapping

import numpy as np
from scipy import sparse
from sklearn.preprocessing import normalize
from joblib import Parallel, delayed
import pandas as pd
from neuroquery.tokenization import TextVectorizer
from neuroquery.datasets import fetch_neuroquery_model

from nqdc._utils import checksum, assert_exists
from nqdc._typing import PathLikeOrStr, BaseProcessingStep
from nqdc import _utils

_LOG = logging.getLogger(__name__)

_FIELDS = ("title", "keywords", "abstract", "body")


class Vocabulary(Enum):
    NEUROQUERY_VOCABULARY = (
        "https://github.com/neuroquery/"
        "neuroquery_data/blob/main/neuroquery_model/vocabulary.csv"
    )


def _get_output_dir(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr],
    vocabulary_file: PathLikeOrStr,
) -> Path:
    """Choose an appropriate output directory & create if necessary."""
    if output_dir is None:
        extracted_data_dir = Path(extracted_data_dir)
        voc_checksum = _checksum_vocabulary(vocabulary_file)
        output_dir_name = re.sub(
            r"^(.*?)(_extractedData)?$",
            rf"\1-voc_{voc_checksum}_vectorizedText",
            extracted_data_dir.name,
        )
        output_dir = extracted_data_dir.with_name(output_dir_name)
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    return output_dir


def vectorize_corpus_to_npz(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    vocabulary: Union[
        PathLikeOrStr, Vocabulary
    ] = Vocabulary.NEUROQUERY_VOCABULARY,
    n_jobs: int = 1,
) -> Tuple[Path, int]:
    """Compute word counts and TFIDF features and store them in `.npz` files.

    Parameters
    ----------
    extracted_data_dir
        The directory containing the text of articles to vectorize. It is a
        directory created by `nqdc.extract_data_to_csv`: it contains a file
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
        0 if previous (data extraction) step was complete and this step
        (vectorization) finished normally as well. Used by the `nqdc`
        command-line interface.
    """
    extracted_data_dir = Path(extracted_data_dir)
    assert_exists(extracted_data_dir.joinpath("text.csv"))
    n_jobs = _utils.check_n_jobs(n_jobs)
    vocabulary_file = _resolve_voc(vocabulary)
    output_dir = _get_output_dir(
        extracted_data_dir, output_dir, vocabulary_file
    )
    status = _utils.check_steps_status(
        extracted_data_dir, output_dir, __name__
    )
    if not status["need_run"]:
        return output_dir, 0
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
        name="vectorization",
        is_complete=is_complete,
        n_articles=n_articles,
    )
    _LOG.info(f"Done creating BOW features .npz files in {output_dir}")
    return output_dir, int(not is_complete)


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
) -> Tuple[np.ndarray, Dict[str, sparse.csr_matrix]]:
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
) -> Tuple[np.ndarray, Dict[str, sparse.csr_matrix], TextVectorizer]:
    """Compute word counts for all articles in a csv file.

    returns the pmcids, mapping of text filed: csr matrix, and the vectorizer.
    order of pmcids matches rows in the feature matrices.
    """
    vectorizer = TextVectorizer.from_vocabulary_file(
        str(vocabulary_file), use_idf=False, norm=None, voc_mapping={}
    ).fit()
    chunksize = 200
    all_chunks = pd.read_csv(
        corpus_file, encoding="utf-8", chunksize=chunksize
    )
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
        directory created by `nqdc.extract_data_to_csv`: it contains a file
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
    voc_mapping = _load_voc_mapping(vocabulary_file)
    pmcids, counts, vectorizer = _extract_word_counts(
        corpus_file, vocabulary_file, n_jobs=n_jobs
    )
    frequencies = {
        k: normalize(v, norm="l1", axis=1, copy=True)
        for k, v in counts.items()
    }
    freq_merged = np.sum(list(frequencies.values())) / len(frequencies)
    frequencies["merged"] = freq_merged
    doc_counts_full_voc = np.asarray((freq_merged > 0).sum(axis=0)).squeeze()
    n_docs = counts["body"].shape[0]
    doc_freq_full_voc = (doc_counts_full_voc + 1) / (n_docs + 1)
    voc = vectorizer.get_feature_names()
    voc_map_mat = _voc_mapping_matrix(voc, voc_mapping)
    counts = {k: v.dot(voc_map_mat.T) for k, v in counts.items()}
    frequencies = {k: v.dot(voc_map_mat.T) for k, v in frequencies.items()}
    doc_counts = np.asarray((frequencies["merged"] > 0).sum(axis=0)).squeeze()
    doc_freq = (doc_counts + 1) / (n_docs + 1)
    idf = -np.log(doc_freq) + 1
    n_terms = len(idf)
    idf_mat = sparse.spdiags(
        idf,
        diags=0,
        m=n_terms,
        n=n_terms,
        format="csr",
    )
    tfidf = {k: v.dot(idf_mat) for k, v in frequencies.items()}
    return {
        "pmcids": pmcids,
        "counts": counts,
        "tfidf": tfidf,
        "document_frequencies_vocabulary": pd.Series(
            doc_freq_full_voc, index=voc
        ),
        "document_frequencies_feature_names": pd.Series(
            doc_freq, index=sorted(set(voc).difference(voc_mapping.keys()))
        ),
        "voc_mapping": voc_mapping,
    }


def _voc_mapping_matrix(
    vocabulary: Sequence[str], voc_mapping: Dict[str, str]
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


def _add_voc_arg(argument_parser: argparse.ArgumentParser) -> None:
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


def _voc_kwarg(voc_file: Optional[str]) -> Dict[str, str]:
    if voc_file is None:
        return {}
    return {"vocabulary": voc_file}


class VectorizationStep(BaseProcessingStep):
    name = "vectorization"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        _add_voc_arg(argument_parser)
        _utils.add_n_jobs_argument(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return vectorize_corpus_to_npz(
            previous_steps_output["data_extraction"],
            n_jobs=args.n_jobs,
            **_voc_kwarg(args.vocabulary_file),
        )


class StandaloneVectorizationStep(BaseProcessingStep):
    name = "vectorization"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        argument_parser.add_argument(
            "extracted_data_dir",
            help="Directory containing the csv file text.csv created by "
            "the nqdc_extract_data command. A sibling directory will be "
            "created for the vectorized data.",
        )
        _add_voc_arg(argument_parser)
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = (
            "Vectorize text by computing word counts and "
            "TFIDF features. The text comes from csv files created by "
            "the nqdc_extract_data command."
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return vectorize_corpus_to_npz(
            args.extracted_data_dir,
            n_jobs=args.n_jobs,
            **_voc_kwarg(args.vocabulary_file),
        )
