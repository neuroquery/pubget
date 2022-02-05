import re

import pytest

import nqdc
from nqdc import _utils


def test_checksum():
    assert _utils.checksum("123") == "202cb962ac59075b964b07152d234b70"
    assert _utils.checksum(b"123") == "202cb962ac59075b964b07152d234b70"


def test_assert_exists(tmp_path):
    _utils.assert_exists(tmp_path)
    tmp_file = tmp_path.joinpath("some_file")
    with pytest.raises(FileNotFoundError):
        _utils.assert_exists(tmp_file)
    tmp_file.touch()
    _utils.assert_exists(tmp_file)


def test_version():
    assert nqdc.__version__ == _utils.get_nqdc_version()
    assert re.match(r"[0-9]+\.[0-9]+\.[0-9]+", _utils.get_nqdc_version())
