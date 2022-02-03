from pathlib import Path
import logging
import tempfile
import re
import argparse
from typing import Dict, Any, Mapping, Tuple, Optional

import numpy as np
from scipy import sparse
import pandas as pd

from nqdc._typing import PathLikeOrStr, BaseProcessingStep

_LOG = logging.getLogger(__name__)


def _get_vocabulary_name(vectorized_dir: Path) -> str:
    match = re.match(r".*-voc_([^-_]+)_vectorizedText", vectorized_dir.name)
    if match is None:
        return "UNKNOWN"
    return match.group(1)


def _get_nimare_dataset_name(vectorized_dir: Path) -> str:
    return re.sub(
        r"^(.*?)(_vectorizedText)?$",
        r"\1_nimareDataset",
        vectorized_dir.name,
    )


def _collapse_authors(authors: pd.DataFrame) -> pd.Series:
    collapsed_authors, author_pmcids = [], []
    for pmcid, article_authors in authors.groupby("pmcid"):
        collapsed_authors.append(
            "; ".join(
                article_authors["surname"]
                .str.cat(article_authors["given-names"], sep=", ", na_rep="")
                .values
            )
        )
        author_pmcids.append(pmcid)
    return pd.Series(collapsed_authors, index=author_pmcids, name="authors")


def _collect_nimare_data(
    extracted_data_dir: Path, vectorized_dir: Path
) -> Dict[str, Any]:
    metadata = pd.read_csv(extracted_data_dir.joinpath("metadata.csv"))
    metadata.rename(
        columns={"pmcid": "id", "publication_year": "year"}, inplace=True
    )
    space = pd.read_csv(extracted_data_dir.joinpath("coordinate_space.csv"))
    metadata["space"] = space["coordinate_space"]
    authors = pd.read_csv(extracted_data_dir.joinpath("authors.csv"))
    collapsed_authors = _collapse_authors(authors)
    metadata = metadata.join(collapsed_authors, on="id")
    coordinates = pd.read_csv(extracted_data_dir.joinpath("coordinates.csv"))
    coordinates.rename(columns={"pmcid": "id"}, inplace=True)
    vocabulary = pd.read_csv(
        vectorized_dir.joinpath("feature_names.csv"), header=None
    )
    tfidf = sparse.load_npz(vectorized_dir.joinpath("merged_tfidf.npz"))
    pmcids = np.loadtxt(str(vectorized_dir.joinpath("pmcid.txt")), dtype=int)
    metadata = metadata.set_index("id").loc[pmcids].reset_index()
    vocabulary_name = _get_vocabulary_name(vectorized_dir)
    return {
        "vocabulary": vocabulary,
        "vocabulary_name": vocabulary_name,
        "tfidf": tfidf,
        "metadata": metadata,
        "coordinates": coordinates,
    }


def _write_nimare_data(
    nimare_data: Mapping[str, Any], tmp_dir: Path
) -> Dict[str, Any]:
    target_metadata = tmp_dir.joinpath("metadata.tsv.gz")
    nimare_data["metadata"].to_csv(target_metadata, sep="\t", index=False)
    target_coordinates = tmp_dir.joinpath("coordinates.tsv.gz")
    nimare_data["coordinates"].to_csv(
        target_coordinates, sep="\t", index=False
    )
    target_vocabulary = tmp_dir.joinpath("vocabulary.txt.gz")
    nimare_data["vocabulary"].iloc[:, 0].to_csv(
        target_vocabulary,
        sep="\t",
        header=None,
        index=False,
    )
    target_features = tmp_dir.joinpath(
        f"vocab-{nimare_data['vocabulary_name']}_source-combined_"
        "type-tfidf_features.npz"
    )
    sparse.save_npz(
        str(target_features),
        nimare_data["tfidf"],
    )
    return {
        "coordinates": str(target_coordinates),
        "metadata": str(target_metadata),
        "annotation_files": [
            {
                "vocabulary": str(target_vocabulary),
                "features": str(target_features),
            }
        ],
    }


def make_nimare_dataset(
    extracted_data_dir: PathLikeOrStr,
    vectorized_dir: PathLikeOrStr,
) -> Tuple[Optional[Path], int]:
    """Create a NiMARE JSON dataset from data collected by `nqdc`.

    See the [NiMARE documentation](https://nimare.readthedocs.io/) for details.
    This function requires `nimare` to be installed.

    Parameters
    ----------
    extracted_data_dir
        The directory containing extracted metadata and coordinates. It is a
        directory created by `nqdc.extract_data_to_csv`.
    vectorized_dir
        The directory containing the vectorized text (TFIDF features). It is
        the directory created by `nqdc.vectorize_corpus_to_npz` using
        `extracted_data_dir` as input.

    Returns
    -------
    output_dir
        The directory in which the NiMARE dataset is stored. It contains a
        `nimare_dataset.json` file.
    exit_code
        0 if the NiMARE was created and 1 otherwise. Used by the `nqdc`
        command-line interface.

    """
    try:
        from nimare.io import convert_neurosynth_to_json
    except ImportError:
        _LOG.error(
            "NiMARE is not installed. Skipping creation of NiMARE dataset."
        )
        return None, 1

    extracted_data_dir = Path(extracted_data_dir)
    vectorized_dir = Path(vectorized_dir)
    output_dir = vectorized_dir.with_name(
        _get_nimare_dataset_name(vectorized_dir)
    )
    _LOG.info(f"Beginning creation of NiMARE dataset in {output_dir}")
    nimare_data = _collect_nimare_data(extracted_data_dir, vectorized_dir)
    with tempfile.TemporaryDirectory() as tmp_dir:
        nimare_params = _write_nimare_data(nimare_data, Path(tmp_dir))
        output_dir.mkdir(exist_ok=True, parents=True)
        convert_neurosynth_to_json(
            nimare_params["coordinates"],
            nimare_params["metadata"],
            str(output_dir.joinpath("nimare_dataset.json")),
            annotations_files=nimare_params["annotation_files"],
        )
    _LOG.info(f"Done creating NiMARE dataset in {output_dir}")
    return output_dir, 0


class NimareStep(BaseProcessingStep):
    name = "nimare"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
        argument_parser.add_argument(
            "--nimare",
            action="store_true",
            help="Create a NiMARE JSON dataset from extracted data. See the "
            "NiMARE documentation for details: https://nimare.readthedocs.io/"
            "en/latest/generated/nimare.dataset.Dataset.html#nimare."
            "dataset.Dataset . This option requires nimare to be installed.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        if not args.nimare:
            return None, 0
        return make_nimare_dataset(
            previous_steps_output["data_extraction"],
            previous_steps_output["vectorization"],
        )
