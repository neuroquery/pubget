"""Client for the Entrez E-utilities needed for downloading articles."""
from pathlib import Path
import logging
from urllib.parse import urljoin
import math
import time
from typing import Optional, Mapping, Union, Dict, Any

import requests

from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


class EntrezClient:
    """Client for esearch and efetch using the pmc database."""

    _default_timeout = 27
    _entrez_base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    _esearch_base_url = urljoin(_entrez_base_url, "esearch.fcgi")
    _efetch_base_url = urljoin(_entrez_base_url, "efetch.fcgi")

    def __init__(
        self,
        request_period: Optional[float] = None,
        api_key: Optional[str] = None,
    ) -> None:
        self._entrez_id = {}
        if api_key is not None:
            self._entrez_id["api_key"] = api_key
        if request_period is None:
            self._request_period = (
                0.15 if "api_key" in self._entrez_id else 1.05
            )
        self._last_request_time: Union[None, float] = None
        self._session = requests.Session()
        self.last_search_result: Optional[Mapping[str, str]] = None
        self.n_failures = 0

    def _wait_to_send_request(self) -> None:
        if self._last_request_time is None:
            self._last_request_time = time.time()
            return
        wait = self._request_period - (time.time() - self._last_request_time)
        if wait > 0:
            _LOG.debug(f"wait for {wait:.3f} seconds to send request")
            time.sleep(wait)
        self._last_request_time = time.time()

    def _send_request(
        self,
        url: str,
        verb: str = "POST",
        params: Optional[Mapping[str, Any]] = None,
        data: Optional[Mapping[str, Any]] = None,
    ) -> Union[None, requests.Response]:
        req = requests.Request(verb, url, params=params, data=data)
        prepped = self._session.prepare_request(req)
        self._wait_to_send_request()
        _LOG.debug(f"sending request: {prepped.url}")
        try:
            resp = self._session.send(prepped, timeout=self._default_timeout)
        except Exception:
            _LOG.exception(f"Request failed: {url}")
            return None
        _LOG.debug(
            f"received response. code: {resp.status_code}; "
            f"reason: {resp.reason}; from: {resp.url}"
        )
        return resp

    def esearch(
        self,
        query: str,
    ) -> Dict[str, str]:
        """Perform search.

        If search fails, returns an empty dictionary. Otherwise returns the
        search results -- keys of interest are "count", "webenv", and
        "querykey".
        """
        search_params = {
            "db": "pmc",
            "term": f"{query}&open+access[filter]",
            "usehistory": "y",
            "retmode": "json",
            "retmax": 5,
        }
        data = {**search_params, **self._entrez_id}
        resp = self._send_request(
            self._esearch_base_url, data=data, verb="POST"
        )
        if resp is None:
            self.n_failures = 1
            return {}
        try:
            search_result: Dict[str, str] = resp.json()["esearchresult"]
        except Exception:
            self.n_failures = 1
            return {}
        if "ERROR" in search_result:
            self.n_failures = 1
            return {}
        self.last_search_result = search_result
        _LOG.info(f"Search returned {search_result['count']} results")
        return search_result

    def _check_search_result(
        self,
        search_result: Optional[Mapping[str, str]] = None,
    ) -> Optional[Mapping[str, str]]:
        if search_result is None:
            search_result = self.last_search_result
        if search_result is None:
            self.n_failures = 1
            return None
        needed_keys = {"count", "webenv", "querykey"}
        if not needed_keys.issubset(search_result.keys()):
            self.n_failures = 1
            _LOG.error(
                "Perform a search before calling `efetch`"
                "or provide `search_result`"
            )
            return None
        return search_result

    def efetch(
        self,
        output_dir: PathLikeOrStr,
        search_result: Optional[Mapping[str, str]] = None,
        n_docs: Optional[int] = None,
        retmax: int = 500,
    ) -> None:
        """Performs the download.

        If not `None`, search_result` must contain "webenv", "querykey" and
        "count". Otherwise a search must have been performed first and the
        results from the search are used.

        This function assumes that the caller (the `_download` module) has made
        sure that if a partial download is in the output directory, it is safe
        to skip already-downloaded batches -- ie the webenv, querykey and
        retmax are the same.
        """
        output_dir = Path(output_dir)
        search_result = self._check_search_result(search_result)
        if search_result is None:
            return
        search_count = int(search_result["count"])
        if n_docs is None:
            n_docs = search_count
        else:
            n_docs = min(n_docs, search_count)
        retstart = 0
        params = {
            "WebEnv": search_result["webenv"],
            "query_key": search_result["querykey"],
            "retmax": retmax,
            "retstart": retstart,
            "db": "pmc",
            **self._entrez_id,
        }
        n_batches = math.ceil(n_docs / retmax)
        self.n_failures = 0
        batch_nb = 0
        _LOG.info(f"Downloading {n_docs} articles (in {n_batches} batches)")
        while retstart < n_docs:
            self._download_batch(output_dir, batch_nb, n_batches, params)
            retstart += retmax
            batch_nb += 1
            params["retstart"] = retstart

    def _download_batch(
        self,
        output_dir: Path,
        batch_nb: int,
        n_batches: int,
        params: Dict[str, Any],
    ) -> None:
        batch_file = output_dir.joinpath(f"articleset_{batch_nb:0>5}.xml")
        if batch_file.is_file():
            _LOG.info(f"batch {batch_nb + 1} already downloaded, skipping")
            return
        _LOG.info(f"getting batch {batch_nb + 1} / {n_batches}")
        resp = self._send_request(
            self._efetch_base_url, verb="POST", data=params
        )
        if resp is None or resp.status_code != 200:
            self.n_failures += 1
            _LOG.error(f"{self.n_failures} batches failed to download")
        else:
            batch_file.write_bytes(resp.content)
