from pathlib import Path
import os
from datetime import datetime
import logging
import logging.config
import sys
import hashlib
from typing import Dict, Any, Union

import configobj
import validate
from lxml import etree

_CONFIG: Union[None, Dict[str, Any]] = None


def configure_logging() -> None:
    log_file = get_config()["log_file"]
    fmt_string = (
        "%(levelname)s\t%(asctime)s\t%(process)d\t%(name)s"
        "\t%(module)s\t%(funcName)s\t%(message)s"
    )
    config = {
        "version": 1,
        "incremental": False,
        "disable_existing_loggers": True,
        "handlers": {
            "console_handler": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "formatter",
            },
            "file_handler": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "formatter",
                "filename": log_file,
                "delay": True,
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
            "handlers": ["console_handler", "file_handler"],
        },
        "loggers": {
            "nqdc": {"propagate": 1, "level": "DEBUG"},
        },
    }
    logging.config.dictConfig(config)
    sys.stderr.write(
        f"{datetime.now().isoformat()}\t{os.getpid()}\tlog file: {log_file}\n"
    )
    logging.captureWarnings(True)


def get_config() -> Dict[str, Any]:
    global _CONFIG
    if _CONFIG is not None:
        return _CONFIG.copy()
    src_dir = Path(__file__).parent
    config_spec = str(src_dir / "data" / "nqdc_spec.conf")
    default_config_path = str(Path.home().joinpath(".nqdc.conf"))
    config_path = os.environ.get("NQDC_CONFIG", default_config_path)
    config = configobj.ConfigObj(
        config_path,
        interpolation="template",
        encoding="UTF-8",
        configspec=config_spec,
    )
    config.validate(validate.Validator())
    config = dict(config)
    for key in config.keys():
        config[key] = os.environ.get(f"NQDC_{key.upper()}", config[key])
    log_dir = Path(config["data_dir"]) / "log"
    log_dir.mkdir(exist_ok=True, parents=True)
    now = datetime.now().isoformat()
    pid = os.getpid()
    log_file = log_dir / f"ukbiobank-{now}-{pid}.log"
    config["log_file"] = str(log_file)
    _CONFIG = config
    return _CONFIG.copy()


def get_data_dir() -> Path:
    return Path(get_config()["data_dir"])


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
