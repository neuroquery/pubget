from pathlib import Path
import logging

from scipy import sparse

import pandas as pd
from neuroquery.tokenization import TextVectorizer

_LOG = logging.getLogger(__name__)


def vectorize_corpus_to_npz(
    corpus_file, vocabulary_file, output_dir, use_idf=False, norm=None
):
    _LOG.info(
        f"vectorizing {corpus_file} using vocabulary "
        f"{vocabulary_file} to {output_dir}"
    )
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    vectorized_fields = vectorize_corpus(
        corpus_file, vocabulary_file, use_idf=False, norm=None
    )
    for field, vectorized in vectorized_fields.items():
        output_file = output_dir / f"{field}.npz"
        sparse.save_npz(str(output_file), vectorized)
    _LOG.info("Done creating BOW features .npz files")


def vectorize_corpus(corpus_file, vocabulary_file, use_idf=False, norm=None):
    vectorizer = TextVectorizer.from_vocabulary_file(
        vocabulary_file, use_idf=use_idf, norm=norm
    ).fit()
    vectorized_chunks = {
        "title": [],
        "keywords": [],
        "abstract": [],
        "body": [],
    }
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
            vectorized_chunks[field], format="csr"
        )
    return vectorized_fields
