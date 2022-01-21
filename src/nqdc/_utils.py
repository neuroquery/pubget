from pathlib import Path
import logging
import logging.config
import hashlib
from datetime import datetime
import os
from typing import Union

from lxml import etree

from nqdc._typing import PathLikeOrStr

_LOG_FORMAT = "%(levelname)s\t%(asctime)s\t%(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def add_log_file(log_dir: PathLikeOrStr, prefix: str = "log_") -> None:
    log_dir = Path(log_dir)
    log_dir.mkdir(exist_ok=True, parents=True)
    log_file = log_dir.joinpath(
        f"{prefix}{datetime.now().isoformat()}_{os.getpid()}"
    )
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
