"""'fit_neuroquery' step: fit a neuroquery.NeuroQueryModel."""
import contextlib
from pathlib import Path
import logging
import argparse
import shutil
import tempfile
import json
from typing import Mapping, Tuple, Optional, Dict

import numpy as np
from scipy import sparse
import pandas as pd
from sklearn.preprocessing import normalize
from nibabel import Nifti1Image

from neuroquery.smoothed_regression import SmoothedRegression
from neuroquery.tokenization import TextVectorizer
from neuroquery.encoding import NeuroQueryModel

from nqdc._typing import PathLikeOrStr, BaseProcessingStep, ArgparseActions
from nqdc import _utils, _img_utils


_LOG = logging.getLogger(__name__)
_STEP_NAME = "fit_neuroquery"
_STEP_DESCRIPTION = "Fit a NeuroQuery encoder on the downloaded data."
_HELP = (
    "Fit a NeuroQuery encoder on the downloaded data. "
    "Note this can be a more computationally intensive step for "
    "large datasets. Moreover, it will not yield "
    "good results for small datasets (less than ~ 5 to 10K articles with "
    "coordinates). See details about neuroquery at neuroquery.org and "
    "https://github.com/neuroquery/neuroquery ."
)


class _NeuroQueryFit:
    """Helper class to load data and fit the NeuroQuery model.

    After creating a _NeuroQueryFit, calling `fit` will
    - load the data
    - compute the article brain maps
    - reindex metadata, tfidf and brain maps so they are indexed by the same
      pmcids
    - drop the terms that are too rare
    - fit and return the NeuroQueryModel
    """

    _MIN_DOCUMENT_FREQUENCY = 10

    def __init__(
        self,
        tfidf_dir: Path,
        extracted_data_dir: Path,
        n_jobs: int,
    ) -> None:
        self.tfidf_dir = tfidf_dir
        self.extracted_data_dir = extracted_data_dir
        self.n_jobs = n_jobs
        self.metadata: Optional[pd.DataFrame] = None
        self.tfidf: Optional[sparse.csr_matrix] = None
        self.coordinates: Optional[pd.DataFrame] = None
        self.brain_maps: Optional[np.memmap] = None
        self.brain_maps_pmcids: Optional[np.ndarray] = None
        self.full_voc: Optional[pd.DataFrame] = None
        self.voc_mapping: Optional[Dict[str, str]] = None
        self.feature_names: Optional[pd.DataFrame] = None
        self.mask_img: Optional[Nifti1Image] = None
        self.encoder: Optional[NeuroQueryModel] = None
        self.context: Optional[contextlib.ExitStack] = None

    def _load_tfidf(self) -> None:
        self.tfidf = sparse.load_npz(
            str(self.tfidf_dir.joinpath("merged_tfidf.npz"))
        )
        self.feature_names = pd.read_csv(
            self.tfidf_dir.joinpath("feature_names.csv"), header=None
        )
        self.full_voc = pd.read_csv(
            self.tfidf_dir.joinpath("vocabulary.csv"), header=None
        )
        self.voc_mapping = json.loads(
            self.tfidf_dir.joinpath(
                "vocabulary.csv_voc_mapping_identity.json"
            ).read_text("utf-8")
        )

    def _load_data(self) -> None:
        self.metadata = pd.read_csv(
            self.extracted_data_dir.joinpath("metadata.csv")
        )
        self.coordinates = pd.read_csv(
            self.extracted_data_dir.joinpath("coordinates.csv")
        )
        self._load_tfidf()

    def _compute_brain_maps(self) -> None:
        assert self.context is not None

        tmp_dir = self.context.enter_context(tempfile.TemporaryDirectory())
        memmap_file = str(Path(tmp_dir).joinpath("brain_maps.dat"))
        _LOG.debug("Computing article maps for NeuroQuery model.")
        brain_maps, pmcids, masker = _img_utils.coordinates_to_memmapped_maps(
            self.coordinates,
            target_affine=(4, 4, 4),
            id_column="pmcid",
            output_memmap_file=memmap_file,
            n_jobs=self.n_jobs,
            context=self.context,
        )
        _LOG.debug("Done computing article maps.")
        non_empty = (brain_maps != 0).any(axis=1)
        self.brain_maps_pmcids = pmcids[non_empty]
        self.brain_maps = brain_maps[non_empty]
        self.mask_img = masker.mask_img_

    def _set_pmcids(self) -> None:
        """Reindex metadata, tfidf and brain maps.

        After this, their rows match (correspond to the same pmcids).
        """
        assert self.metadata is not None
        assert self.brain_maps is not None
        assert self.brain_maps_pmcids is not None
        assert self.tfidf is not None

        tfidf_pmcids = self.metadata["pmcid"].values
        pmcids = np.asarray(
            sorted(set(self.brain_maps_pmcids).intersection(tfidf_pmcids))
        )
        maps_rindex = pd.Series(
            np.arange(len(self.brain_maps_pmcids)),
            index=self.brain_maps_pmcids,
        )
        self.brain_maps = self.brain_maps[maps_rindex.loc[pmcids].values, :]

        rindex = pd.Series(np.arange(len(tfidf_pmcids)), index=tfidf_pmcids)
        self.tfidf = sparse.csr_matrix(
            self.tfidf.A[rindex.loc[pmcids].values, :]
        )

        self.metadata.set_index("pmcid", inplace=True)
        # false positive: pylint thinks read_csv returns a TextFileReader
        # pylint: disable-next=no-member
        self.metadata = self.metadata.loc[pmcids, :]
        # pylint: disable-next=no-member
        self.metadata.index.name = "pmcid"
        # pylint: disable-next=no-member
        self.metadata.reset_index(inplace=True)

    def _filter_out_rare_terms(self) -> None:
        """Drop very rare terms, update tfidf and vocabulary."""
        assert self.tfidf is not None
        assert self.full_voc is not None
        assert self.feature_names is not None
        assert self.voc_mapping is not None

        kept = np.asarray(
            (self.tfidf > 0).sum(axis=0) > self._MIN_DOCUMENT_FREQUENCY
        ).ravel()
        self.tfidf = self.tfidf[:, kept]
        self.feature_names = self.feature_names[kept]
        feat_names_set = set(self.feature_names.iloc[:, 0].values)
        self.voc_mapping = {
            source: target
            for (source, target) in self.voc_mapping.items()
            if target in feat_names_set
        }
        voc_set = feat_names_set.union(self.voc_mapping.keys())
        self.full_voc = self.full_voc[self.full_voc.iloc[:, 0].isin(voc_set)]

    def _fit_regression(self) -> None:
        """Actual fitting of the NeuroQuerymodel."""
        assert self.full_voc is not None
        assert self.tfidf is not None

        normalize(self.tfidf, norm="l2", axis=1, copy=False)
        regressor = SmoothedRegression()
        _LOG.debug(f"Fitting NeuroQuery on {self.tfidf.shape[0]} samples.")
        regressor.fit(self.tfidf, self.brain_maps)
        _LOG.debug("Done fitting NeuroQuery model.")
        # false positive: pylint thinks read_csv returns a TextFileReader
        vectorizer = TextVectorizer.from_vocabulary(
            # pylint: disable-next=no-member
            self.full_voc.iloc[:, 0].values,
            # pylint: disable-next=no-member
            self.full_voc.iloc[:, 1].values,
            voc_mapping=self.voc_mapping,
            norm="l2",
        )
        self.encoder = NeuroQueryModel(
            vectorizer,
            regressor,
            self.mask_img,
            corpus_info={
                "tfidf": self.tfidf,
                "metadata": self.metadata,
            },
        )

    def fit(self) -> NeuroQueryModel:
        """Return a fitted NeuroQueryModel."""
        with contextlib.ExitStack() as self.context:
            self._load_data()
            self._compute_brain_maps()
            self._set_pmcids()
            self._filter_out_rare_terms()
            self._fit_regression()
        return self.encoder


def _copy_static_files(output_dir: Path) -> None:
    module_data = _utils.get_package_data_dir().joinpath("_fit_neuroquery")
    for file_name in "app.py", "requirements.txt", "README.md":
        shutil.copy(module_data.joinpath(file_name), output_dir)


def fit_neuroquery(
    tfidf_dir: PathLikeOrStr,
    extracted_data_dir: Optional[PathLikeOrStr] = None,
    output_dir: Optional[PathLikeOrStr] = None,
    n_jobs: int = 1,
) -> Tuple[Path, int]:
    """Fit a NeuroQuery encoder.

    Parameters
    ----------
    vectorized_dir
        The directory containing the vectorized text (TFIDF features). It is
        the directory created by `nqdc.vectorize_corpus_to_npz` using
        `extracted_data_dir` as input.
    extracted_data_dir
        The directory containing extracted metadata and coordinates. It is a
        directory created by `nqdc.extract_data_to_csv`. If `None`, this
        function looks for a sibling directory of the `vectorized_dir` whose
        name ends with `_extractedData`.
    output_dir
        Directory in which to store the NeuroQuery model. If not specified, a
        sibling directory of `vectorized_dir` whose name ends with
        `_neuroqueryModel` is created. It will contain a `neuroquery_model`
        subdirectory that can be loaded with
        `neuroquery.NeuroQueryModel.from_data_dir`

    Returns
    -------
    output_dir
        The directory in which the neuroquery model is stored.
    exit_code
        0 if the neuroquery model was fitted and 1 otherwise. Used by the
        `nqdc` command-line interface.

    """
    tfidf_dir = Path(tfidf_dir)
    extracted_data_dir = _utils.get_extracted_data_dir_from_tfidf_dir(
        tfidf_dir, extracted_data_dir
    )
    output_dir = _utils.get_output_dir(
        tfidf_dir, output_dir, "_vectorizedText", "_neuroqueryModel"
    )
    status = _utils.check_steps_status(tfidf_dir, output_dir, __name__)
    if not status["need_run"]:
        return output_dir, 0
    _LOG.info(
        f"Training a NeuroQuery encoder with data from {tfidf_dir} "
        f"and {extracted_data_dir}."
    )
    encoder = _NeuroQueryFit(
        tfidf_dir,
        extracted_data_dir,
        n_jobs,
    ).fit()
    model_dir = output_dir.joinpath("neuroquery_model")
    encoder.to_data_dir(model_dir)
    _LOG.info(f"NeuroQuery model saved in {model_dir}.")
    _copy_static_files(output_dir)
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(output_dir, name=_STEP_NAME, is_complete=is_complete)
    return output_dir, 0


class FitNeuroQueryStep(BaseProcessingStep):
    """Fitting NeuroQuery model as part of a pipeline (nqdc run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "--fit_neuroquery",
            action="store_true",
            help=_HELP,
        )
        _utils.add_n_jobs_argument(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        if not args.fit_neuroquery:
            return None, 0
        return fit_neuroquery(
            previous_steps_output["vectorize"],
            previous_steps_output["extract_data"],
            n_jobs=args.n_jobs,
        )


class StandaloneFitNeuroqueryStep(BaseProcessingStep):
    """Fitting NeuroQuery as a standalone command (nqdc fit_neuroquery)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "vectorized_data_dir",
            help="Directory containing TFIDF features and vocabulary. "
            "It is a directory created by nqdc whose name ends with "
            "'_vectorizedText'. A sibling directory will be created for "
            "the NeuroQuery model.",
        )
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = _HELP

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return fit_neuroquery(args.vectorized_data_dir, n_jobs=args.n_jobs)
