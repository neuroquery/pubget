from pathlib import Path
import logging
import logging.config
import hashlib
import json
from datetime import datetime
import os
from typing import Union

from lxml import etree

from nqdc._typing import PathLikeOrStr

_LOG_FORMAT = "%(levelname)s\t%(asctime)s\t%(module)s\t%(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def timestamp() -> str:
    return datetime.now().isoformat().replace(":", "-")


def add_log_file(log_dir: PathLikeOrStr, prefix: str = "log_") -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir.joinpath(f"{prefix}{timestamp()}_{os.getpid()}")
    logger = logging.getLogger("")
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def configure_logging() -> None:
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


def is_step_complete(step_dir: Path, step_name: str) -> bool:
    info_file = step_dir.joinpath("info.json")
    if not info_file.is_file():
        return False
    info = json.loads(info_file.read_text("utf-8"))
    return bool(info.get(f"{step_name}_complete"))
