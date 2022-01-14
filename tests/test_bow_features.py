import numpy as np

from nqdc import _bow_features


def test_voc_mapping_matrix():
    voc = ["amygdala", "brain stem", "brainstem", "cortex"]
    mapping = {"brain stem": "brainstem"}
    op = _bow_features._voc_mapping_matrix(voc, mapping)
    assert np.allclose(op.A, [[1, 0, 0, 0], [0, 1, 1, 0], [0, 0, 0, 1]])
    assert np.allclose(op.dot(np.arange(1, len(voc) + 1)), [1, 5, 4])
