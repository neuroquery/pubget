import logging
from pathlib import Path
import re

import numpy as np
import pandas as pd
import joblib
from scipy import stats, sparse
from typing import Optional, Tuple

from nqdc import _model_fit_utils, _img_utils, _utils
from nqdc._typing import PathLikeOrStr  # , BaseProcessingStep, ArgparseActions

_LOG = logging.getLogger(__name__)
_STEP_NAME = "fit_neurosynth"


def _chi_square(brain_maps: np.memmap, term_vector: np.ndarray) -> np.ndarray:
    term_vector = term_vector.astype("int32")
    n_studies = brain_maps.shape[0]
    observed = np.empty((2, 2, brain_maps.shape[1]))
    term = term_vector.sum()
    noterm = n_studies - term
    vox = brain_maps.sum(axis=0)
    novox = n_studies - vox

    observed[1, 1, :] = term_vector.dot(brain_maps)
    observed[0, 1, :] = (1 - term_vector).dot(brain_maps)
    observed[1, 0, :] = term - observed[1, 1, :]
    observed[0, 0, :] = noterm - observed[0, 1, :]

    expected = np.empty(observed.shape)
    expected[1, 1, :] = term * vox / n_studies
    expected[0, 1, :] = noterm * vox / n_studies
    expected[1, 0, :] = term * novox / n_studies
    expected[0, 0, :] = noterm * novox / n_studies
    expected_0 = expected == 0
    expected[expected_0] = 1

    cells = (observed - expected) ** 2 / expected
    cells[expected_0] = 0
    stat = np.sum(cells, axis=(0, 1))
    # degrees of freedom = (2 - 1) * (2 - 1) = 1
    z_values: np.ndarray = stats.norm().isf(stats.chi2(1).sf(stat) / 2)
    return z_values


def _compute_meta_analysis_map(
    output_dir: Path,
    term: str,
    brain_maps: np.memmap,
    masker: _img_utils.NiftiMasker,
    tfidf_vector: sparse.csr_matrix,
    tfidf_threshold: float,
) -> None:
    term_map = _chi_square(
        brain_maps, np.asarray(tfidf_vector.A).ravel() > tfidf_threshold
    )
    img = masker.inverse_transform(term_map)
    term_name = re.sub(r"\W", "_", term)
    img_path = output_dir.joinpath(f"{term_name}.nii.gz")
    img.to_filename(str(img_path))


class _NeuroSynthFit(_model_fit_utils.DataManager):
    """Helper class to load data and fit the NeuroQuery model."""

    _BRAIN_MAP_DTYPE = "int8"
    # TODO _VOXEL_SIZE = 2.0
    _VOXEL_SIZE = 4.0
    _TFIDF_THRESHOLD = 0.001

    def __init__(
        self,
        output_dir: Path,
        tfidf_dir: Path,
        extracted_data_dir: Path,
        n_jobs: int,
    ) -> None:
        super().__init__(
            tfidf_dir=tfidf_dir,
            extracted_data_dir=extracted_data_dir,
            n_jobs=n_jobs,
        )
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True, parents=True)

    @staticmethod
    def _img_filter(
        coordinates: pd.DataFrame,
        masker: _img_utils.NiftiMasker,
        output: np.memmap,
        idx: int,
    ) -> None:
        _img_utils._ball_coords_to_masked_map(coordinates, masker, output, idx)

    def _fit_model(self) -> None:
        assert self.feature_names is not None
        assert self.tfidf is not None

        joblib.Parallel(self.n_jobs, verbose=1)(
            joblib.delayed(_compute_meta_analysis_map)(
                self.output_dir,
                term,
                self.brain_maps,
                self.masker,
                term_tfidf,
                self._TFIDF_THRESHOLD,
            )
            for term, term_tfidf in zip(
                self.feature_names.iloc[:, 0], self.tfidf.T
            )
        )


def fit_neurosynth(
    tfidf_dir: PathLikeOrStr,
    extracted_data_dir: Optional[PathLikeOrStr] = None,
    output_dir: Optional[PathLikeOrStr] = None,
    n_jobs: int = 1,
) -> Tuple[Path, int]:
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
        f"Performing NeuroSynth analysis with data from {tfidf_dir} "
        f"and {extracted_data_dir}."
    )
    _NeuroSynthFit(output_dir, tfidf_dir, extracted_data_dir, n_jobs).fit()
    _LOG.info(f"NeuroSynth results saved in {output_dir}.")
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(output_dir, name=_STEP_NAME, is_complete=is_complete)
    return output_dir, 0
