import shutil
from unittest.mock import Mock
import json

import pandas as pd

from nqdc import _vocabulary


def test_extract_vocabulary(tmp_path, test_data_dir, monkeypatch):
    input_dir = tmp_path.joinpath("subset-allArticles_extractedData")
    shutil.copytree(test_data_dir, input_dir)
    input_dir.joinpath("info.json").write_text(
        json.dumps({"is_complete": True}), "utf-8"
    )
    voc = _vocabulary.extract_vocabulary(input_dir)
    assert len(voc) == 11
    assert voc["auditory cortex"] == 0.5
    output_dir = tmp_path.joinpath("subset-allArticles_extractedVocabulary")
    _vocabulary.extract_vocabulary_to_csv(input_dir, None)
    voc = pd.read_csv(
        output_dir.joinpath("vocabulary.csv"), header=None, index_col=0
    ).squeeze("columns")
    assert len(voc) == 11
    assert voc["auditory cortex"] == 0.5
    mock = Mock()
    monkeypatch.setattr(_vocabulary, "extract_vocabulary", mock)
    _, code = _vocabulary.extract_vocabulary_to_csv(input_dir, output_dir)
    assert code == 0
    assert not mock.called
