from pathlib import Path
import os
from datetime import datetime
import logging
import logging.config
import sys
import hashlib


import configobj
import validate


def _configure_logging():
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


def get_config():
    if getattr(get_config, "config", None) is not None:
        return get_config.config
    src_dir = Path(__file__).parent
    config_spec = str(src_dir / "nqdc_spec.conf")
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
    get_config.config = config
    _configure_logging()
    return config


def get_data_dir():
    return Path(get_config()["data_dir"])


def hash(value):
    if isinstance(value, str):
        value = value.encode("utf-8")
    return hashlib.md5(value).hexdigest()
