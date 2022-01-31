from pathlib import Path
import re
import logging
import json
from enum import Enum
from typing import Tuple, Dict, Any, Sequence, List, Optional, Union

import numpy as np
from scipy import sparse
from sklearn.preprocessing import normalize

import pandas as pd
from neuroquery.tokenization import TextVectorizer
from neuroquery.datasets import fetch_neuroquery_model

from nqdc._utils import checksum, assert_exists
from nqdc._typing import PathLikeOrStr
from nqdc import _utils

_LOG = logging.getLogger(__name__)


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
    vocabulary_file = _resolve_voc(vocabulary)
    output_dir = _get_output_dir(
        extracted_data_dir, output_dir, vocabulary_file
    )
    _LOG.info(
        f"vectorizing {extracted_data_dir} using vocabulary "
        f"{vocabulary_file} to {output_dir}"
    )
    if _utils.is_step_complete(output_dir, "vectorization"):
        _LOG.info("Vectorization complete, nothing to do.")
        return output_dir, 0
    data_extraction_complete = _utils.is_step_complete(
        extracted_data_dir, "data_extraction"
    )
    if not data_extraction_complete:
        _LOG.warning(
            "Data extraction is incomplete, not all articles "
            "matching query will be vectorized."
        )
    n_articles = _do_vectorize_corpus_to_npz(
        extracted_data_dir, output_dir, vocabulary_file
    )
    output_dir.joinpath("info.json").write_text(
        json.dumps(
            {
                "vectorization_complete": data_extraction_complete,
                "n_articles": n_articles,
            }
        ),
        "utf-8",
    )
    _LOG.info(f"Done creating BOW features .npz files in {output_dir}")
    return output_dir, int(not data_extraction_complete)


def _do_vectorize_corpus_to_npz(
    extracted_data_dir: Path, output_dir: Path, vocabulary_file: Path
) -> int:
    extraction_result = vectorize_corpus(extracted_data_dir, vocabulary_file)
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


def _get_n_articles_msg(corpus_file: PathLikeOrStr) -> str:
    try:
        n_articles = json.loads(
            Path(corpus_file).with_name("info.json").read_text("utf-8")
        )["n_articles"]
        n_articles_msg = f" / {n_articles}"
    except Exception:
        n_articles_msg = ""
    return n_articles_msg


def _extract_word_counts(
    corpus_file: PathLikeOrStr, vocabulary_file: PathLikeOrStr
) -> Tuple[np.ndarray, Dict[str, sparse.csr_matrix], TextVectorizer]:
    vectorizer = TextVectorizer.from_vocabulary_file(
        str(vocabulary_file), use_idf=False, norm=None, voc_mapping={}
    ).fit()
    vectorized_chunks: Dict[str, List[sparse.csr_matrix]] = {
        "title": [],
        "keywords": [],
        "abstract": [],
        "body": [],
    }
    chunksize = 200
    pmcids = []
    n_articles_msg = _get_n_articles_msg(corpus_file)
    for i, chunk in enumerate(
        pd.read_csv(corpus_file, encoding="utf-8", chunksize=chunksize)
    ):
        _LOG.debug(
            f"vectorizing articles {i * chunksize} to "
            f"{i * chunksize + chunk.shape[0]}{n_articles_msg}"
        )
        chunk.fillna("", inplace=True)
        for field in vectorized_chunks:
            vectorized_chunks[field].append(
                vectorizer.transform(chunk[field].values)
            )
        pmcids.append(chunk["pmcid"].values)
    vectorized_fields = {}
    for field in vectorized_chunks:
        vectorized_fields[field] = sparse.vstack(
            vectorized_chunks[field], format="csr", dtype=int
        )
    return np.concatenate(pmcids), vectorized_fields, vectorizer


def _get_voc_mapping_file(vocabulary_file: PathLikeOrStr) -> Path:
    return Path(f"{vocabulary_file}_voc_mapping_identity.json")


def _checksum_vocabulary(vocabulary_file: PathLikeOrStr) -> str:
    voc = Path(vocabulary_file).read_bytes()
    voc_mapping_file = _get_voc_mapping_file(vocabulary_file)
    if voc_mapping_file.is_file():
        voc += voc_mapping_file.read_bytes()
    return checksum(voc)


def _load_voc_mapping(vocabulary_file: PathLikeOrStr) -> Dict[str, str]:
    voc_mapping_file = _get_voc_mapping_file(vocabulary_file)
    if voc_mapping_file.is_file():
        voc_mapping: Dict[str, str] = json.loads(
            voc_mapping_file.read_text("utf-8")
        )
    else:
        voc_mapping = {}
    return voc_mapping


def _get_neuroquery_vocabulary() -> Path:
    return Path(fetch_neuroquery_model()).joinpath("vocabulary.csv")


def _resolve_voc(vocabulary: Union[PathLikeOrStr, Vocabulary]) -> Path:
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

    Returns
    -------
    vectorized_data
        Contains the pmcids of the vectorized articles, the document
        frequencies of the vocabulary, and the word counts and TFIDF for each
        article section and for whole articles as scipy sparse matrices.
    """
    corpus_file = Path(extracted_data_dir).joinpath("text.csv")
    assert_exists(corpus_file)
    vocabulary_file = _resolve_voc(vocabulary)
    voc_mapping = _load_voc_mapping(vocabulary_file)
    pmcids, counts, vectorizer = _extract_word_counts(
        corpus_file, vocabulary_file
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
    word_to_idx = pd.Series(np.arange(len(vocabulary)), index=vocabulary)
    form = sparse.eye(len(vocabulary), format="lil", dtype=int)
    keep = np.ones(len(vocabulary), dtype=bool)
    for source, target in voc_mapping.items():
        s_idx, t_idx = word_to_idx[source], word_to_idx[target]
        keep[s_idx] = False
        form[t_idx, s_idx] = 1
    form = form[keep, :]
    return form.tocsr()
