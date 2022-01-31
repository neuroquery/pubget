import logging
import json
from pathlib import Path
from typing import Optional, Tuple

from nqdc._entrez import EntrezClient
from nqdc import _utils
from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


def download_articles_for_query(
    query: str,
    data_dir: PathLikeOrStr,
    *,
    n_docs: Optional[int] = None,
    retmax: int = 500,
    api_key: Optional[str] = None,
) -> Tuple[Path, int]:
    """Download full-text articles matching the given query.

    Parameters
    ----------
    query
        Search term for querying the PMC database. You can build the query
        using the [PMC advanced search
        interface](https://www.ncbi.nlm.nih.gov/pmc/advanced). For more
        information see [the E-Utilities
        help](https://www.ncbi.nlm.nih.gov/books/NBK3837/).
    data_dir
        Path to the directory where all nqdc data is stored; a subdirectory
        will be created for this query.
    n_docs
        Approximate maximum number of articles to download. By default, all
        results returned for the search are downloaded. If n_docs is specified,
        at most n_docs rounded up to the nearest multiple of 500 articles will
        be downloaded.
    retmax
        Batch size -- number of articles that are downloaded per request.
    api_key
        API key for the Entrez E-utilities (see [the E-utilities
        help](https://www.ncbi.nlm.nih.gov/books/NBK25497/)). If the API key is
        provided, it is included in all requests to the Entrez E-utilities.

    Returns
    -------
    output_dir
        The directory that was created in which downloaded data is stored.
    exit_code
        0 if all articles matching the search have been successfully downloaded
        and 1 otherwise. Used by the `nqdc` command-line interface.
    """
    data_dir = Path(data_dir)
    output_dir = data_dir.joinpath(
        f"query-{_utils.checksum(query)}", "articlesets"
    )
    info_file = output_dir.joinpath("info.json")
    if info_file.is_file():
        info = json.loads(info_file.read_text("utf-8"))
        if info["download_complete"]:
            _LOG.info("Download already complete, nothing to do")
            return output_dir, 0
    else:
        output_dir.mkdir(exist_ok=True, parents=True)
        info = {
            "query": query,
            "retmax": retmax,
            "download_complete": False,
        }
    _LOG.info(f"Downloading data in {output_dir}")
    client = EntrezClient(api_key=api_key)
    if "search_result" in info:
        _LOG.info(
            "Found partial download, resuming download of webenv "
            f"{info['search_result']['webenv']}, "
            f"query key {info['search_result']['querykey']}"
        )
    else:
        _LOG.info("Performing search")
        info["search_result"] = client.esearch(query)
        info_file.write_text(json.dumps(info), "utf-8")
    client.efetch(
        output_dir,
        search_result=info["search_result"],
        n_docs=n_docs,
        retmax=info["retmax"],
    )
    _LOG.info(f"Finished downloading articles in {output_dir}")
    info["download_complete"] = client.n_failures == 0 and (
        n_docs is None or n_docs >= int(info["search_result"]["count"])
    )
    if info["download_complete"]:
        _LOG.info("All articles matching the query have been downloaded")
    else:
        _LOG.warning(
            "Download is incomplete -- not all articles matching "
            "the query have been downloaded"
        )
    info_file.write_text(json.dumps(info), "utf-8")
    return output_dir, int(client.n_failures != 0)
