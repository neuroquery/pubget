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
    """.strip()
    buf = io.BytesIO(mask_text)
    mask = np.loadtxt(buf, dtype="int8")
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
