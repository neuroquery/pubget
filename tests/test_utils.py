import json
import re
from unittest.mock import Mock

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


@pytest.mark.parametrize(
    ("n_jobs", "cpu_count", "expected"),
    [(1, None, 1), (8, 4, 4), (-1, 8, 8), (-1, None, 1), (-2, 4, 1)],
)
def test_check_n_jobs(n_jobs, cpu_count, expected, monkeypatch):
    monkeypatch.setattr("os.cpu_count", Mock(return_value=cpu_count))
    assert _utils.check_n_jobs(n_jobs) == expected


def test_get_n_articles(tmp_path):
    info = tmp_path.joinpath("info.json")
    assert _utils.get_n_articles(tmp_path) is None
    info.write_text("text", "utf-8")
    assert _utils.get_n_articles(tmp_path) is None
    info.write_text(json.dumps({"is_complete": True}), "utf-8")
    assert _utils.get_n_articles(tmp_path) is None
    info.write_text(
        json.dumps({"is_complete": True, "n_articles": 23}), "utf-8"
    )
    assert _utils.get_n_articles(tmp_path) == 23


@pytest.mark.parametrize(
    "input_dir_name, output_dir_name, suffix_to_remove,"
    " suffix_to_add, expected_name",
    [
        ("articles", "user_defined", "articles", "addthis", "user_defined"),
        ("articles", None, "articles", "addthis", "addthis"),
        ("articles_", None, "articles", "addthis", "articles_addthis"),
    ],
)
def test_get_output_dir(
    input_dir_name,
    output_dir_name,
    suffix_to_remove,
    suffix_to_add,
    expected_name,
    tmp_path,
):
    input_dir = tmp_path.joinpath(input_dir_name)
    if output_dir_name is not None:
        output_dir = tmp_path.joinpath(output_dir_name)
    else:
        output_dir = None
    chosen = _utils.get_output_dir(
        input_dir, output_dir, suffix_to_remove, suffix_to_add
    )
    assert chosen == input_dir.with_name(expected_name)
    assert chosen.is_dir()
