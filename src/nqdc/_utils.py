from pathlib import Path
import logging
import logging.config
import hashlib
from typing import Union

from lxml import etree


def configure_logging() -> None:
    fmt_string = "%(levelname)s\t%(asctime)s\t%(message)s"
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
                "format": fmt_string,
                "datefmt": "%Y-%m-%dT%H:%M:%S%z",
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


def get_package_data_dir() -> Path:
    return Path(__file__).parent / "data"


def hash(value: Union[str, bytes]) -> str:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.md5(value).hexdigest()


def load_stylesheet(stylesheet_name: str) -> etree.XSLT:
    stylesheet_path = get_package_data_dir() / "stylesheets" / stylesheet_name
    stylesheet_xml = etree.parse(str(stylesheet_path))
    transform = etree.XSLT(stylesheet_xml)
    return transform
