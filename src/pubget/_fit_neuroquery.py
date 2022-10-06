"""'fit_neuroquery' step: fit a neuroquery.NeuroQueryModel."""
import argparse
import logging
import shutil
from pathlib import Path
from typing import Mapping, Optional, Tuple

from neuroquery.encoding import NeuroQueryModel
from neuroquery.smoothed_regression import SmoothedRegression
from neuroquery.tokenization import TextVectorizer
from sklearn.preprocessing import normalize

from pubget import _model_data, _utils
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    PathLikeOrStr,
    PipelineStep,
)

_LOG = logging.getLogger(__name__)
_STEP_NAME = "fit_neuroquery"
_STEP_DESCRIPTION = "Fit a NeuroQuery encoder on the downloaded data."
_STEP_HELP = (
    "Fit a NeuroQuery encoder on the downloaded data. "
    "This can be a more computationally intensive step for "
    "large datasets. Moreover, it will not yield "
    "good results for small datasets (less than ~ 5 to 10K articles with "
    "coordinates). See details about neuroquery at neuroquery.org and "
    "https://github.com/neuroquery/neuroquery ."
)


def _do_fit_neuroquery(
    tfidf_dir: Path,
    extracted_data_dir: Path,
    n_jobs: int,
) -> NeuroQueryModel:
    """Do the actual work of fitting the encoder."""
    with _model_data.ModelData(
        tfidf_dir=tfidf_dir,
        extracted_data_dir=extracted_data_dir,
        n_jobs=n_jobs,
    ) as data:
        assert data.full_voc is not None
        assert data.tfidf is not None
        assert data.masker is not None

        tfidf = normalize(data.tfidf, norm="l2", axis=1)
        regressor = SmoothedRegression()
        _LOG.debug(f"Fitting NeuroQuery on {tfidf.shape[0]} samples.")
        regressor.fit(tfidf, data.brain_maps)
        _LOG.debug("Done fitting NeuroQuery model.")
        # false positive: pylint thinks read_csv returns a TextFileReader
        vectorizer = TextVectorizer.from_vocabulary(
            # pylint: disable-next=no-member
            data.full_voc["term"].values,
            # pylint: disable-next=no-member
            data.full_voc["document_frequency"].values,
            voc_mapping=data.voc_mapping,
            norm="l2",
        )
        encoder = NeuroQueryModel(
            vectorizer,
            regressor,
            data.masker.mask_img_,
            corpus_info={
                "tfidf": tfidf,
                "metadata": data.metadata,
            },
        )
        return encoder


def fit_neuroquery(
    tfidf_dir: PathLikeOrStr,
    extracted_data_dir: Optional[PathLikeOrStr] = None,
    output_dir: Optional[PathLikeOrStr] = None,
    n_jobs: int = 1,
) -> Tuple[Path, ExitCode]:
    """Fit a NeuroQuery encoder.

    Parameters
    ----------
    vectorized_dir
        The directory containing the vectorized text (TFIDF features). It is
        the directory created by `pubget.vectorize_corpus_to_npz` using
        `extracted_data_dir` as input.
    extracted_data_dir
        The directory containing extracted metadata and coordinates. It is a
        directory created by `pubget.extract_data_to_csv`. If `None`, this
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
        COMPLETED if the neuroquery model was fitted and previous steps were
        complete and INCOMPLETE otherwise. Used by the `pubget` command-line
        interface.
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
        return output_dir, ExitCode.COMPLETED
    _LOG.info(
        f"Training a NeuroQuery encoder with data from {tfidf_dir} "
        f"and {extracted_data_dir}."
    )
    encoder = _do_fit_neuroquery(
        tfidf_dir,
        extracted_data_dir,
        n_jobs,
    )
    model_dir = output_dir.joinpath("neuroquery_model")
    if model_dir.exists():
        shutil.rmtree(model_dir)
    encoder.to_data_dir(model_dir)
    _LOG.info(f"NeuroQuery model saved in {model_dir}.")
    _utils.copy_static_files("_fit_neuroquery", output_dir)
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(output_dir, name=_STEP_NAME, is_complete=is_complete)
    exit_code = ExitCode.COMPLETED if is_complete else ExitCode.INCOMPLETE
    return output_dir, exit_code


class FitNeuroQueryStep(PipelineStep):
    """Fitting NeuroQuery model as part of a pipeline (pubget run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "--fit_neuroquery",
            action="store_true",
            help=_STEP_HELP,
        )
        _utils.add_n_jobs_argument(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], ExitCode]:
        if not args.fit_neuroquery:
            return None, ExitCode.COMPLETED
        return fit_neuroquery(
            previous_steps_output["vectorize"],
            previous_steps_output["extract_data"],
            n_jobs=args.n_jobs,
        )


class FitNeuroQueryCommand(Command):
    """Fitting NeuroQuery as a standalone command (pubget fit_neuroquery)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "vectorized_data_dir",
            help="Directory containing TFIDF features and vocabulary. "
            "It is a directory created by pubget whose name ends with "
            "'_vectorizedText'. A sibling directory will be created for "
            "the NeuroQuery model.",
        )
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = _STEP_HELP

    def run(
        self,
        args: argparse.Namespace,
    ) -> ExitCode:
        return fit_neuroquery(args.vectorized_data_dir, n_jobs=args.n_jobs)[1]
