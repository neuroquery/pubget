"""Utilities for manipulating images.

They are mostly copied and adapted from neuroquery, and made to allow choosing
smoothing kernel and output type for neurosynth.

It may be possible to remove them and import directly from neuroquery after
some future neuroquery release.

"""
import contextlib
from typing import Callable, Optional, Tuple

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from neuroquery.img_utils import coords_to_peaks_img, get_masker
from nilearn import image
from scipy import ndimage

from pubget._typing import NiftiMasker, PathLikeOrStr

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
    output: np.ndarray,
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
    output: np.ndarray,
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
    img_filter: Callable[[pd.DataFrame, NiftiMasker, np.ndarray, int], None],
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


def tal_coordinates_to_mni(
    coordinates: pd.DataFrame, coordinate_spaces: pd.DataFrame
) -> pd.DataFrame:
    """Transform the TAL coordinates to MNI space.

    Coordinates in MNI or UNKNOWN space are not modified.

    A new dataframe is returned containing the transformed coordinates, the
    original is left unchanged.

    The parameters of the affine transformation are taken from the
    nimare.utils.tal2mni function (nimare 0.0.11), itself based on:

        "the tal2icbm transform developed and validated by Jack Lancaster at
        the Research Imaging Center in San Antonio, Texas.
        http://www3.interscience.wiley.com/cgi-bin/abstract/114104479/ABSTRACT"


    Parameters
    ----------
    coordinates: pmcid and x, y, z coordinates
    coordinate_spaces: index is the pmcid and "coordinate_space" column is the
        coordinate space, "TAL" for Talairach. All pmcids in coordinates must
        be contained in the index.

    Returns
    -------
    A dataframe of the same shape as `coordinates` where the TAL coordinates
    have been transformed to MNI.

    """
    transform = np.linalg.inv(
        [
            [0.9357, 0.0029, -0.0072, -1.0423],
            [-0.0065, 0.9396, -0.0726, -1.3940],
            [0.0103, 0.0752, 0.8967, 3.6475],
            [0.0000, 0.0000, 0.0000, 1.0000],
        ]
    )
    spaces = coordinate_spaces.loc[
        coordinates[_ID_COLUMN_NAME].values, "coordinate_space"
    ].values
    tal_idx = coordinates.index[spaces == "TAL"]
    tal_coords = coordinates.loc[tal_idx, ["x", "y", "z"]].values
    transformed_coords = transform[:3, :3].dot(tal_coords.T).T
    transformed_coords += transform[:3, -1]
    new_coords = coordinates.copy()
    new_coords.loc[tal_idx, ["x", "y", "z"]] = transformed_coords
    return new_coords
