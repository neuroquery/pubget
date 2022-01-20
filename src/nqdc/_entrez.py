import re
import logging
from urllib.parse import urljoin
import math
import time
from typing import Optional, Mapping, Union, Dict, Any, Generator

import requests

_LOG = logging.getLogger(__name__)


class EntrezClient:
    _default_timeout = 10
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
        term: str,
    ) -> Dict[str, str]:
        search_params = {
            "db": "pmc",
            "term": f"{term}&open+access[filter]",
            "usehistory": "y",
            "retmode": "json",
            "retmax": 5,
        }
        data = {**search_params, **self._entrez_id}
        resp = self._send_request(
            self._esearch_base_url, data=data, verb="POST"
        )
        if resp is None:
            return {}
        try:
            search_info: Dict[str, str] = resp.json()["esearchresult"]
        except Exception:
            return {}
        if "ERROR" in search_info:
            return {}
        self.last_search_result = search_info
        return search_info

    def _check_search_info(self) -> None:
        needed_keys = {"count", "webenv", "querykey"}
        if not hasattr(self, "last_search_result") or not needed_keys.issubset(
            self.last_search_result.keys()
        ):
            raise ValueError(
                "Perform a search before calling `efetch`"
                "or provide `search_info`"
            )

    def efetch(
        self,
        search_info: Optional[Mapping[str, str]] = None,
        n_docs: Optional[int] = None,
        retmax: int = 500,
    ) -> Generator[bytes, None, None]:
        self._check_search_info()
        search_info = self.last_search_result
        search_count = int(search_info["count"])
        if n_docs is None:
            n_docs = search_count
        else:
            n_docs = min(n_docs, search_count)
        retmax = min(n_docs, retmax)
        retstart = 0
        params = {
            "WebEnv": search_info["webenv"],
            "query_key": search_info["querykey"],
            "retmax": retmax,
            "retstart": retstart,
            "db": "pmc",
            **self._entrez_id,
        }
        n_batches = math.ceil(n_docs / retmax)
        n_failures = 0
        while retstart < n_docs:
            _LOG.debug(
                f"getting batch {(retstart // retmax) + 1} / {n_batches}"
            )
            resp = self._send_request(
                self._efetch_base_url, verb="POST", data=params
            )
            if resp is None or resp.status_code != 200:
                n_failures += 1
                _LOG.error(f"{n_failures} batches failed to download")
            else:
                yield resp.content
            retstart += retmax
            params["retstart"] = retstart
