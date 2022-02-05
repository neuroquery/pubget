from pathlib import Path
import logging
import logging.config
import hashlib
import json
from datetime import datetime
import os
from typing import Union, Optional, Dict, Any

from lxml import etree

from nqdc._typing import PathLikeOrStr

_LOG_FORMAT = "%(levelname)s\t%(asctime)s\t%(name)s\t%(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def get_nqdc_version() -> str:
    return (
        Path(__file__)
        .parent.joinpath("data", "VERSION")
        .read_text("utf-8")
        .strip()
    )


def timestamp() -> str:
    return datetime.now().isoformat().replace(":", "-")


def _add_log_file(
    log_dir: Optional[PathLikeOrStr] = None, log_filename_prefix: str = "log_"
) -> None:
    if log_dir is None:
        log_dir = os.environ.get("NQDC_LOG_DIR", None)
    if log_dir is None:
        return
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir.joinpath(
        f"{log_filename_prefix}{timestamp()}_{os.getpid()}"
    )
    logger = logging.getLogger("")
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def configure_logging(
    log_dir: Optional[PathLikeOrStr] = None, log_filename_prefix: str = "log_"
) -> None:
    config = {
        "version": 1,
        "incremental": False,
        "disable_existing_loggers": False,
        "handlers": {
            "console_handler": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
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
            "nqdc": {"propagate": 1, "level": "DEBUG"},
        },
    }
    logging.config.dictConfig(config)
    logging.captureWarnings(True)
    _add_log_file(log_dir, log_filename_prefix)


def checksum(value: Union[str, bytes]) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.md5(value).hexdigest()


def load_stylesheet(stylesheet_name: str) -> etree.XSLT:
    stylesheet_path = Path(__file__).parent.joinpath(
        "data", "stylesheets", stylesheet_name
    )
    stylesheet_xml = etree.parse(str(stylesheet_path))
    transform = etree.XSLT(stylesheet_xml)
    return transform


def get_pmcid(article: Union[etree.ElementTree, etree.Element]) -> int:
    return int(
        article.find("front/article-meta/article-id[@pub-id-type='pmc']").text
    )


def assert_exists(path: Path) -> None:
    path.resolve(strict=True)


def check_steps_status(
    previous_step_dir: Optional[Path], current_step_dir: Path, logger_name: str
) -> Dict[str, Union[None, bool, str]]:
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
    info["name"] = name
    info["is_complete"] = is_complete
    info["date"] = datetime.now().isoformat()
    info["nqdc_version"] = get_nqdc_version()
    info_file = output_dir.joinpath("info.json")
    info_file.write_text(json.dumps(info), "utf-8")
    return info_file
