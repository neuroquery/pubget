"""'fit_neuroquery' step: fit a neuroquery.NeuroQueryModel."""
from pathlib import Path
import logging
import argparse
import shutil
from typing import Optional, Mapping, Tuple

from sklearn.preprocessing import normalize

from neuroquery.smoothed_regression import SmoothedRegression
from neuroquery.tokenization import TextVectorizer
from neuroquery.encoding import NeuroQueryModel

from nqdc._typing import PathLikeOrStr, BaseProcessingStep, ArgparseActions
from nqdc import _utils, _model_fit_utils


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


class _NeuroQueryFit(_model_fit_utils.DataManager):
    """Helper class to load data and fit the NeuroQuery model."""

    def __init__(
        self,
        tfidf_dir: Path,
        extracted_data_dir: Path,
        n_jobs: int,
    ) -> None:
        super().__init__(
            tfidf_dir=tfidf_dir,
            extracted_data_dir=extracted_data_dir,
            n_jobs=n_jobs,
        )
        self.encoder: Optional[NeuroQueryModel] = None

    def _fit_model(self) -> None:
        """Actual fitting of the NeuroQuerymodel."""
        assert self.full_voc is not None
        assert self.tfidf is not None
        assert self.masker is not None

        normalize(self.tfidf, norm="l2", axis=1, copy=False)
        regressor = SmoothedRegression()
        _LOG.debug(f"Fitting NeuroQuery on {self.tfidf.shape[0]} samples.")
        regressor.fit(self.tfidf, self.brain_maps)
        _LOG.debug("Done fitting NeuroQuery model.")
        # false positive: pylint thinks read_csv returns a TextFileReader
        vectorizer = TextVectorizer.from_vocabulary(
            # pylint: disable-next=no-member
            self.full_voc["term"].values,
            # pylint: disable-next=no-member
            self.full_voc["document_frequency"].values,
            voc_mapping=self.voc_mapping,
            norm="l2",
        )
        self.encoder = NeuroQueryModel(
            vectorizer,
            regressor,
            self.masker.mask_img_,
            corpus_info={
                "tfidf": self.tfidf,
                "metadata": self.metadata,
            },
        )

    def get_fitted_model(self) -> NeuroQueryModel:
        """Fit and return the NeuroQuery encoder."""
        self.fit()
        assert self.encoder is not None
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
    ).get_fitted_model()
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


class StandaloneFitNeuroQueryStep(BaseProcessingStep):
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
