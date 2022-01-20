import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from nqdc._entrez import EntrezClient
from nqdc import _utils
from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


def download_articles_for_search_term(
    term: str,
    data_dir: PathLikeOrStr,
    n_docs: Optional[int] = None,
    retmax: int = 500,
    api_key: Optional[str] = None,
) -> Path:
    data_dir = Path(data_dir)
    output_dir = data_dir.joinpath(f"query-{_utils.hash(term)}")
    info_file = output_dir.joinpath("info.json")
    if info_file.is_file():
        info = json.loads(info_file.read_text("utf-8"))
        if info["download_complete"]:
            _LOG.info("Download already complete, nothing to do")
            return output_dir
    else:
        output_dir.mkdir(exist_ok=True, parents=True)
        info = {
            "term": term,
            "retmax": retmax,
            "download_complete": False,
        }
    _LOG.info(f"Downloading data in {output_dir}")
    _LOG.info("Performing search")
    client = EntrezClient(api_key=api_key)
    if "search_result" in info:
        _LOG.info(
            "Found partial download, resuming download of webenv "
            f"{info['search_result']['webenv']}, "
            f"query key {info['search_result']['querykey']}"
        )
    else:
        info["search_result"] = client.esearch(term)
        info_file.write_text(json.dumps(info), "utf-8")
    client.efetch(
        output_dir,
        search_result=info["search_result"],
        n_docs=n_docs,
        retmax=info["retmax"],
    )
    _LOG.info("Finished downloading articles")
    info["download_complete"] = client.n_failures == 0 and (
        n_docs is None or n_docs >= int(info["search_result"]["count"])
    )
    info_file.write_text(json.dumps(info), "utf-8")
    return output_dir
