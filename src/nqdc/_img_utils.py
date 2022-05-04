"""Utilities for manipulating images.

They are mostly copied and adapted from neuroquery, and it may be possible to
remove them and import directly from neuroquery after the next neuroquery
release (version 1.0.3).

"""
import contextlib
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from nibabel import Nifti1Image
from nilearn import image

try:
    from nilearn import maskers
# import only used for type annotations, was called input_data in old nilearn
# versions
except ImportError:  # pragma: nocover
    from nilearn import input_data as maskers
from neuroquery.img_utils import get_masker, coords_to_peaks_img

from nqdc._typing import PathLikeOrStr


def _coords_to_masked_map(
    coordinates: pd.DataFrame,
    masker: maskers.NiftiMasker,
    fwhm: float,
    output: np.memmap,
    idx: int,
) -> None:
    peaks_img = coords_to_peaks_img(coordinates, mask_img=masker.mask_img_)
    img = image.smooth_img(peaks_img, fwhm=fwhm)
    output[idx] = masker.transform(img).squeeze()


def coordinates_to_memmapped_maps(
    coordinates: pd.DataFrame,
    output_memmap_file: PathLikeOrStr,
    mask_img: Optional[Nifti1Image] = None,
    target_affine: Tuple[float, float, float] = (4.0, 4.0, 4.0),
    fwhm: float = 9.0,
    n_jobs: int = 1,
    id_column: str = "pmcid",
    context: Optional[contextlib.ExitStack] = None,
) -> Tuple[np.memmap, np.ndarray, maskers.NiftiMasker]:
    """Transform coordinates into (masked) brain images stored in a memmap."""
    masker = get_masker(mask_img=mask_img, target_affine=target_affine)
    article_ids = np.unique(coordinates[id_column].values)
    shape = len(article_ids), image.get_data(masker.mask_img_).sum()
    output = np.memmap(
        str(output_memmap_file),
        mode="w+",
        dtype=np.float32,
        shape=shape,
    )
    if context is not None:
        # mypy complains no attribute _mmap but it does have one
        # moreover we have to use this private attribute it is the only way to
        # close a numpy memmap.
        # pylint: disable-next=protected-access
        context.enter_context(contextlib.closing(output._mmap))  # type: ignore
    all_articles = coordinates.groupby(id_column, sort=True)
    Parallel(n_jobs, verbose=1)(
        delayed(_coords_to_masked_map)(
            article.loc[:, ["x", "y", "z"]].values,
            masker,
            fwhm,
            output,
            i,
        )
        for i, (art_id, article) in enumerate(all_articles)
    )
    output.flush()
    return output, article_ids, masker
