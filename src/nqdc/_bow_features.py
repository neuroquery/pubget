from pathlib import Path
import logging
import json
from typing import Tuple, Dict, Any, Sequence, List

import numpy as np
from scipy import sparse
from sklearn.preprocessing import normalize

import pandas as pd
from neuroquery.tokenization import TextVectorizer

from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


def vectorize_corpus_to_npz(
    corpus_file: PathLikeOrStr,
    vocabulary_file: PathLikeOrStr,
    output_dir: PathLikeOrStr,
) -> None:
    _LOG.info(
        f"vectorizing {corpus_file} using vocabulary "
        f"{vocabulary_file} to {output_dir}"
    )
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    extraction_result = vectorize_corpus(corpus_file, vocabulary_file)
    for feature_kind in "counts", "tfidf":
        for field, vectorized in extraction_result[feature_kind].items():
            output_file = output_dir / f"{field}_{feature_kind}.npz"
            sparse.save_npz(str(output_file), vectorized)
    for voc_name in "feature_names", "vocabulary":
        voc_file = output_dir / f"{voc_name}.csv"
        extraction_result[f"document_frequencies_{voc_name}"].to_csv(
            voc_file, header=None
        )
    voc_mapping_file = output_dir / "vocabulary.csv_voc_mapping_identity.json"
    voc_mapping_file.write_text(
        json.dumps(extraction_result["voc_mapping"]), "utf-8"
    )
    _LOG.info(f"Done creating BOW features .npz files in {output_dir}")


def _extract_word_counts(
    corpus_file: PathLikeOrStr, vocabulary_file: PathLikeOrStr
) -> Tuple[Dict[str, sparse.csr_matrix], TextVectorizer]:
    vectorizer = TextVectorizer.from_vocabulary_file(
        str(vocabulary_file), use_idf=False, norm=None, voc_mapping={}
    ).fit()
    vectorized_chunks = {
        "title": [],
        "keywords": [],
        "abstract": [],
        "body": [],
    }  # type: Dict[str, List[sparse.csr_matrix]]
    for i, chunk in enumerate(
        pd.read_csv(corpus_file, encoding="utf-8", chunksize=1000)
    ):
        _LOG.debug(
            f"transforming articles {i * 1000} to {i * 1000 + chunk.shape[0]}"
        )
        chunk.fillna("", inplace=True)
        for field in vectorized_chunks:
            vectorized_chunks[field].append(
                vectorizer.transform(chunk[field].values)
            )
    vectorized_fields = {}
    for field in vectorized_chunks:
        vectorized_fields[field] = sparse.vstack(
            vectorized_chunks[field], format="csr", dtype=int
        )
    return vectorized_fields, vectorizer


def _load_voc_mapping(vocabulary_file: PathLikeOrStr) -> Dict[str, str]:
    voc_mapping_file = Path(f"{vocabulary_file}_voc_mapping_identity.json")
    if voc_mapping_file.is_file():
        voc_mapping: Dict[str, str] = json.loads(
            voc_mapping_file.read_text("utf-8")
        )
    else:
        voc_mapping = {}
    return voc_mapping


def vectorize_corpus(
    corpus_file: PathLikeOrStr, vocabulary_file: PathLikeOrStr
) -> Dict[str, Any]:
    voc_mapping = _load_voc_mapping(vocabulary_file)
    counts, vectorizer = _extract_word_counts(corpus_file, vocabulary_file)
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
