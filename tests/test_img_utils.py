import io
import contextlib

import numpy as np
import pandas as pd
from nilearn import datasets

try:
    from nilearn import maskers
except ImportError:
    from nilearn import input_data as maskers
import neuroquery
import neuroquery.img_utils

from nqdc import _img_utils


def _array_from_text(text, dtype):
    buf = io.BytesIO(text)
    return np.loadtxt(buf, dtype=dtype)


def _neuroquery_coordinates_to_maps(
    coordinates, output_memmap_file, n_jobs, context
):
    return _img_utils.coordinates_to_memmapped_maps(
        coordinates,
        output_memmap_file,
        output_dtype="float32",
        img_filter=_img_utils.gaussian_coords_to_masked_map,
        target_affine=(4.0, 4.0, 4.0),
        n_jobs=n_jobs,
        context=context,
    )


def _neurosynth_coordinates_to_maps(
    coordinates,
    output_memmap_file,
    n_jobs,
    context,
):
    return _img_utils.coordinates_to_memmapped_maps(
        coordinates,
        output_memmap_file,
        output_dtype="int8",
        img_filter=_img_utils.ball_coords_to_masked_map,
        target_affine=(2.0, 2.0, 2.0),
        n_jobs=n_jobs,
        context=context,
    )


def test_ball_kernel():
    mask_text = b"""
    0 0 0 0 0 0 0
    0 0 1 1 1 0 0
    0 1 1 1 1 1 0
    0 1 1 1 1 1 0
    0 1 1 1 1 1 0
    0 0 1 1 1 0 0
    0 0 0 0 0 0 0
    """
    mask = _array_from_text(mask_text, "int8")
    ball = _img_utils._ball_kernel(10, 3).astype("int8")
    assert (ball[2, :, :] == mask).all()
    assert (ball[:, 2, :] == mask).all()
    assert (ball[:, :, 2] == mask).all()


def test_neuroquery_coordinates_to_maps(tmp_path):
    # compare to neuroquery upstream version
    coords = pd.DataFrame.from_dict(
        {
            "pmid": [3, 17, 17, 2, 2],
            "x": [0.0, 0.0, 10.0, 5.0, 3.0],
            "y": [0.0, 0.0, -10.0, 15.0, -9.0],
            "z": [27.0, 0.0, 30.0, 17.0, 177.0],
        }
    )
    coords["pmcid"] = coords["pmid"]
    ref_maps, _ = neuroquery.img_utils.coordinates_to_maps(coords)
    memmap = tmp_path.joinpath("maps.dat")
    with contextlib.ExitStack() as context:
        maps_vals, pmids, masker = _neuroquery_coordinates_to_maps(
            coords, output_memmap_file=memmap, context=context, n_jobs=1
        )
        assert isinstance(maps_vals, np.memmap)
        maps = pd.DataFrame(maps_vals, index=pmids, copy=True)
    assert np.allclose(maps.values, ref_maps.values)
    assert (maps.index.values == ref_maps.index.values).all()


def test_neurosynth_coordinates_to_maps(tmp_path):
    coords = pd.DataFrame.from_dict(
        {
            "pmcid": [3, 17, 17, 2, 2],
            "x": [0.0, 0.0, 10.0, 5.0, 3.0],
            "y": [0.0, 0.0, -10.0, 15.0, -9.0],
            "z": [27.0, 0.0, 30.0, 17.0, 177.0],
        }
    )
    memmap = tmp_path.joinpath("maps.dat")
    with contextlib.ExitStack() as context:
        maps_vals, pmcids, masker = _neurosynth_coordinates_to_maps(
            coords, output_memmap_file=memmap, context=context, n_jobs=1
        )
        assert isinstance(maps_vals, np.memmap)
        maps = pd.DataFrame(maps_vals, index=pmcids, copy=True)
    assert maps.shape == (3, 235375)
    coords_17 = [(0.0, 0.0, 0.0), (10.0, -10.0, 30.0)]
    masker = maskers.NiftiMasker(datasets.load_mni152_brain_mask()).fit()
    spheres_masker = maskers.NiftiSpheresMasker(
        coords_17, mask_img=masker.mask_img_, radius=10.0
    ).fit()
    img = spheres_masker.inverse_transform(np.asarray([1, 1]))
    masked = masker.transform(img)
    # don't perfectly overlap because NiftiSphereMasker's spheres are not
    # exactly round
    assert (masked != maps.loc[17].values).sum() <= 300


def test_tal_coordinates_to_mni():
    coords_text = b"""
    63.70 26.98  4.10
    1.65 81.33 91.28
    60.66 72.95 54.36
    93.51 81.59  0.27
    85.74  3.36 72.97
    """
    coords_vals = _array_from_text(coords_text, float)
    coords = pd.DataFrame(coords_vals, columns=list("xyz"))
    coords["pmcid"] = np.arange(coords.shape[0]) % 2
    spaces = pd.DataFrame({"coordinate_space": ["TAL", "TAL"]}, index=[1, 0])
    new_coords = _img_utils.tal_coordinates_to_mni(coords, spaces)
    # computed with NiMARE 0.0.11 nimare.utils.tal2mni
    nimare_coords_text = b"""
     69.075033    30.456140    -2.842951
      3.273290    94.997018    89.723441
     66.059616    83.351088    48.805714
    100.682157    88.063905   -12.308388
     93.289343    11.520897    75.270713
    """
    nimare_coords = _array_from_text(nimare_coords_text, float)
    assert np.allclose(new_coords.loc[:, list("xyz")].values, nimare_coords)
