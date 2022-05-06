"""Utilities for manipulating images.

They are mostly copied and adapted from neuroquery, and made to allow choosing
smoothing kernel and output type for neurosynth.

It may be possible to remove them and import directly from neuroquery after
some future neuroquery release.

"""
import contextlib
from typing import Optional, Tuple, Callable

import numpy as np
import pandas as pd
from scipy import ndimage
from joblib import Parallel, delayed
from nilearn import image


from neuroquery.img_utils import get_masker, coords_to_peaks_img
from nqdc._typing import PathLikeOrStr, NiftiMasker

_ID_COLUMN_NAME = "pmcid"
_GAUSSIAN_SMOOTHING_FWHM_MM = 9.0
_BALL_SMOOTHING_RADIUS_MM = 10.0


def _ball_kernel(radius_mm: float, voxel_size_mm: float) -> np.ndarray:
    radius_voxels = max(1, int(radius_mm / voxel_size_mm))
    grid = np.mgrid[
        -radius_voxels : radius_voxels + 1,
        -radius_voxels : radius_voxels + 1,
        -radius_voxels : radius_voxels + 1,
    ]
    mask: np.ndarray = (grid**2).sum(axis=0) < radius_voxels**2
    return mask


def ball_coords_to_masked_map(
    coordinates: pd.DataFrame,
    masker: NiftiMasker,
    output: np.memmap,
    idx: int,
) -> None:
    """Smooth peaks with hard ball.

    Resulting image is masked and stored in `output`.
    """
    radius_mm = _BALL_SMOOTHING_RADIUS_MM
    voxel_size = np.abs(masker.mask_img_.affine[0, 0])
    peaks_img = coords_to_peaks_img(coordinates, mask_img=masker.mask_img_)
    peaks_data = image.get_data(peaks_img).astype(bool)
    kernel = _ball_kernel(radius_mm, voxel_size)
    smoothed = ndimage.maximum_filter(
        peaks_data, footprint=kernel, mode="constant"
    )
    output[idx] = smoothed[image.get_data(masker.mask_img_).astype(bool)]


def gaussian_coords_to_masked_map(
    coordinates: pd.DataFrame,
    masker: NiftiMasker,
    output: np.memmap,
    idx: int,
) -> None:
    """Smooth peaks with Gaussian kernel.

    Resulting image is masked and stored in `output`.
    """
    fwhm = _GAUSSIAN_SMOOTHING_FWHM_MM
    peaks_img = coords_to_peaks_img(coordinates, mask_img=masker.mask_img_)
    img = image.smooth_img(peaks_img, fwhm=fwhm)
    output[idx] = masker.transform(img).squeeze()


def coordinates_to_memmapped_maps(
    coordinates: pd.DataFrame,
    output_memmap_file: PathLikeOrStr,
    *,
    output_dtype: str,
    img_filter: Callable[[pd.DataFrame, NiftiMasker, np.memmap, int], None],
    target_affine: Tuple[float, float, float],
    n_jobs: int,
    context: Optional[contextlib.ExitStack],
) -> Tuple[np.memmap, np.ndarray, NiftiMasker]:
    """Transform coordinates into (masked) brain images stored in a memmap."""
    masker = get_masker(mask_img=None, target_affine=target_affine)
    article_ids = np.unique(coordinates[_ID_COLUMN_NAME].values)
    shape = len(article_ids), image.get_data(masker.mask_img_).sum()
    output = np.memmap(
        str(output_memmap_file),
        mode="w+",
        dtype=output_dtype,
        shape=shape,
    )
    if context is not None:
        # mypy complains no attribute _mmap but it does have one
        # moreover we have to use this private attribute it is the only way to
        # close a numpy memmap.
        # pylint: disable-next=protected-access
        context.enter_context(contextlib.closing(output._mmap))  # type: ignore
    all_articles = coordinates.groupby(_ID_COLUMN_NAME, sort=True)
    Parallel(n_jobs, verbose=1)(
        delayed(img_filter)(
            article.loc[:, ["x", "y", "z"]].values,
            masker,
            output,
            i,
        )
        for i, (art_id, article) in enumerate(all_articles)
    )
    output.flush()
    return output, article_ids, masker
