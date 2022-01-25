import pytest

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
