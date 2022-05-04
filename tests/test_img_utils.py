import contextlib
import numpy as np
import pandas as pd
import neuroquery
import neuroquery.img_utils

from nqdc import _img_utils


def test_coordinates_to_memmapped_maps(tmp_path):
    # compare to neuroquery upstream version
    coords = pd.DataFrame.from_dict(
        {
            "pmid": [3, 17, 17, 2, 2],
            "x": [0.0, 0.0, 10.0, 5.0, 3.0],
            "y": [0.0, 0.0, -10.0, 15.0, -9.0],
            "z": [27.0, 0.0, 30.0, 17.0, 177.0],
        }
    )
    ref_maps, _ = neuroquery.img_utils.coordinates_to_maps(coords)
    memmap = tmp_path.joinpath("maps.dat")
    with contextlib.ExitStack() as context:
        maps_vals, pmids, masker = _img_utils.coordinates_to_memmapped_maps(
            coords,
            output_memmap_file=memmap,
            context=context,
            id_column="pmid",
        )
        assert isinstance(maps_vals, np.memmap)
        maps = pd.DataFrame(maps_vals, index=pmids, copy=True)
    assert np.allclose(maps.values, ref_maps.values)
    assert (maps.index.values == ref_maps.index.values).all()
