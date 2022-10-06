"""Various utility functions for internal use."""
import argparse
import functools
import hashlib
import json
import logging
import logging.config
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Tuple, Union

import pandas as pd
from lxml import etree

from pubget._typing import ArgparseActions, PathLikeOrStr

_LOG = logging.getLogger(__name__)

_LOG_FORMAT = "%(levelname)s\t%(asctime)s\t%(name)s\t%(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def get_package_data_dir() -> Path:
    """Path of the pubget package data.

    (data distributed with the code; not the same thing as data downloaded by
    pubget.)

    """
    return Path(__file__).with_name("_data")


def get_pubget_version() -> str:
    """Find the package version."""
    return (
        get_package_data_dir().joinpath("VERSION").read_text("utf-8").strip()
    )


def timestamp() -> str:
    """Timestamp that can be used in a file name."""
    return datetime.now().isoformat().replace(":", "-")


def _add_log_file(
    log_dir: Optional[PathLikeOrStr] = None,
    log_filename_prefix: str = "pubget_log_",
) -> None:
    """Add a file log handler if user specified a pubget log directory."""
    if log_dir is None:
        log_dir = os.environ.get("PUBGET_LOG_DIR", None)
    if log_dir is None:
        return
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir.joinpath(
        f"{log_filename_prefix}{timestamp()}_{os.getpid()}"
    )
    logger = logging.getLogger("")
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def configure_logging(
    log_dir: Optional[PathLikeOrStr] = None, log_filename_prefix: str = "log_"
) -> None:
    """Add logging handlers.

    Only used by the commands in `pubget._commands` -- handlers are added when
    `pubget` is used as a command-line tool but not when it is imported as a
    library.
    """
    config = {
        "version": 1,
        "incremental": False,
        "disable_existing_loggers": False,
        "handlers": {
            "console_handler": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "formatter",
            },
        },
        "formatters": {
            "formatter": {
                "format": _LOG_FORMAT,
                "datefmt": _LOG_DATE_FORMAT,
            }
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console_handler"],
        },
        "loggers": {
            "pubget": {"propagate": 1, "level": "DEBUG"},
        },
    }
    logging.config.dictConfig(config)
    logging.captureWarnings(True)
    _add_log_file(log_dir, log_filename_prefix)


def checksum(value: Union[str, bytes]) -> str:
    """MD5 checksum of utf-8 encoded string."""
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.md5(value).hexdigest()


def article_bucket_from_pmcid(pmcid: int) -> str:
    """Get the bucket name (in 'articles' dir) from PMCID."""
    return checksum(str(pmcid))[:3]


# functools.cache is new in python3.9
@functools.lru_cache(maxsize=None)
def load_stylesheet(stylesheet_name: str) -> etree.XSLT:
    """Find and parse an XSLT stylesheet."""
    stylesheet_path = get_package_data_dir().joinpath(
        "stylesheets", stylesheet_name
    )
    stylesheet_xml = etree.parse(str(stylesheet_path))
    transform = etree.XSLT(stylesheet_xml)
    return transform


def get_pmcid(article: Union[etree.ElementTree, etree.Element]) -> int:
    """Extract the PubMedCentral ID from an XML article."""
    return int(
        article.find("front/article-meta/article-id[@pub-id-type='pmc']").text
    )


def get_pmcid_from_article_dir(article_dir: Path) -> int:
    """Extract the PubMedCentral ID from an article's data dir."""
    match = re.match(r"pmcid_(\d+)", article_dir.name)
    assert match is not None
    return int(match.group(1))


def read_article_table(
    table_info_json: Path,
) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Load information and data for an article table.

    Takes care to create a MultiIndex if the table had several header rows.
    Returns a tuple (table metadata, table data).
    """
    table_info = json.loads(table_info_json.read_text("UTF-8"))
    table_csv = table_info_json.with_name(table_info["table_data_file"])
    table_data = pd.read_csv(
        table_csv, header=list(range(table_info["n_header_rows"]))
    )
    return table_info, table_data


def get_tables_from_article_dir(
    article_dir: Path,
) -> Generator[Tuple[Dict[str, Any], pd.DataFrame], None, None]:
    """Load information and data for all tables belonging to an article."""
    for table_info_json in sorted(
        article_dir.joinpath("tables").glob("table_*_info.json")
    ):
        yield read_article_table(table_info_json)


def assert_exists(path: Path) -> None:
    """raise a FileNotFoundError if path doesn't exist."""
    path.resolve(strict=True)


def check_steps_status(
    previous_step_dir: Optional[Path], current_step_dir: Path, logger_name: str
) -> Dict[str, Union[None, bool, str]]:
    """Check whethere previous and current processing steps are complete.

    Logs a warning if the previous step is incomplete and a message about
    skipping the current step if it is already complete.

    Returns a dict with completion status of both steps. "need_run" is true if
    the current step is not already complete. If `previous_step_dir` is `None`
    it means there is no previous step (the current step is download, the
    beginning of the pipeline).
    """
    result: Dict[str, Union[None, bool, str]] = dict.fromkeys(
        [
            "previous_step_complete",
            "current_step_complete",
            "previous_step_name",
            "current_step_name",
            "need_run",
        ]
    )
    if previous_step_dir is not None:
        assert_exists(previous_step_dir)
        previous_info_file = previous_step_dir.joinpath("info.json")
        if previous_info_file.is_file():
            previous_info = json.loads(previous_info_file.read_text("utf-8"))
            result["previous_step_complete"] = previous_info["is_complete"]
            result["previous_step_name"] = previous_info.get(
                "name", previous_step_dir.name
            )
        else:
            result["previous_step_complete"] = False
            result["previous_step_name"] = previous_step_dir.name
    current_info_file = current_step_dir.joinpath("info.json")
    if current_info_file.is_file():
        current_info = json.loads(current_info_file.read_text("utf-8"))
        result["current_step_complete"] = current_info["is_complete"]
        result["current_step_name"] = current_info.get(
            "name", current_step_dir.name
        )
    else:
        result["current_step_complete"] = False
        result["current_step_name"] = current_step_dir.name
    logger = logging.getLogger(logger_name)
    if result["current_step_complete"]:
        logger.info(
            f"Nothing to do: current processing step "
            f"'{result['current_step_name']}' already completed "
            f"in {current_step_dir}"
        )
        result["need_run"] = False
        return result
    if previous_step_dir is not None and not result["previous_step_complete"]:
        logger.warning(
            f"Previous processing step '{result['previous_step_name']}' "
            "was not completed: not all the articles matching the query "
            "will be processed."
        )
    result["need_run"] = True
    return result


def write_info(
    output_dir: Path, *, name: str, is_complete: bool, **info: Any
) -> Path:
    """Write info about a processing step to its output directory."""
    info["name"] = name
    info["is_complete"] = is_complete
    info["date"] = datetime.now().isoformat()
    info["pubget_version"] = get_pubget_version()
    info_file = output_dir.joinpath("info.json")
    info_file.write_text(json.dumps(info), "utf-8")
    return info_file


def get_n_articles(data_dir: Path) -> Optional[int]:
    """get `n_articles` reported in a processing step's output dir."""
    try:
        return int(
            json.loads(data_dir.joinpath("info.json").read_text("utf-8"))[
                "n_articles"
            ]
        )
    except Exception:
        return None


def add_n_jobs_argument(argument_parser: ArgparseActions) -> None:
    """Add n_jobs to command-line arguments if it is not already there."""
    try:
        argument_parser.add_argument(
            "--n_jobs",
            type=int,
            default=1,
            help="Number of processes to run in parallel. "
            "-1 means use all processors.",
        )
    except argparse.ArgumentError:
        pass


def check_n_jobs(n_jobs: int) -> int:
    """Choose the number of processes to use."""
    cpu_count = os.cpu_count()
    if n_jobs == -1:
        return cpu_count if cpu_count is not None else 1
    if n_jobs < 1:
        _LOG.error(f"n_jobs set to invalid value '{n_jobs}'; using 1 instead.")
        return 1
    if cpu_count is not None:
        return min(n_jobs, cpu_count)
    return n_jobs


def get_output_dir(
    input_dir: Path,
    output_dir: Optional[PathLikeOrStr],
    suffix_to_remove: str,
    suffix_to_add: str,
) -> Path:
    """Choose an appropriate output directory & create if necessary."""
    if output_dir is None:
        output_dir_name = re.sub(
            rf"^(.*?)({suffix_to_remove})?$",
            rf"\1{suffix_to_add}",
            input_dir.name,
        )
        output_dir = input_dir.with_name(output_dir_name)
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    return output_dir


def get_extracted_data_dir_from_tfidf_dir(
    tfidf_dir: Path, extracted_data_dir: Optional[PathLikeOrStr]
) -> Path:
    """Find extracted_data_dir if not specified."""
    if extracted_data_dir is None:
        dir_name = re.sub(
            r"^(.*)-voc_.*_vectorizedText",
            r"\1_extractedData",
            tfidf_dir.name,
        )
        found_data_dir = tfidf_dir.with_name(dir_name)
    else:
        found_data_dir = Path(extracted_data_dir)
    assert_exists(found_data_dir)
    return found_data_dir


def copy_static_files(input_dir_name: str, output_dir: Path) -> None:
    """Copy all files in a directory under package data to output directory."""
    data_dir = get_package_data_dir().joinpath(input_dir_name)
    for static_file in data_dir.glob("*"):
        if static_file.is_file():
            shutil.copy(static_file, output_dir)
