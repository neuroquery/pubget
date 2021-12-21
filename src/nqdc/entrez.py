import logging
from urllib.parse import urljoin
import math
import time

import requests

from nqdc.utils import get_config

_LOG = logging.getLogger(__name__)


class EntrezClient:
    _default_timeout = 10
    _entrez_base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    _esearch_base_url = urljoin(_entrez_base_url, "esearch.fcgi")
    _efetch_base_url = urljoin(_entrez_base_url, "efetch.fcgi")

    def __init__(self, request_period=None):
        self._entrez_id = {"tool": "neuroquery_data_collection"}
        config = get_config()
        if config["email"] != "":
            self._entrez_id["email"] = config["email"]
        if request_period is None:
            self._request_period = 0.01 if "email" in self._entrez_id else 1
        self._last_request_time = None
        self._session = requests.Session()
        self._session.params = self._entrez_id
        self._session.timeout = self._default_timeout

    def _wait_to_send_request(self):
        if self._last_request_time is None:
            self._last_request_time = time.time()
            return
        wait = self._request_period - (time.time() - self._last_request_time)
        if wait > 0:
            _LOG.debug(f"wait for {wait:.3f} seconds to send request")
            time.sleep(wait)
        self._last_request_time = time.time()

    def _send_request(self, url, params, verb="GET"):
        req = requests.Request(verb, url, params=params)
        prepped = self._session.prepare_request(req)
        self._wait_to_send_request()
        _LOG.debug(f"sending request: {prepped.url}")
        try:
            resp = self._session.send(prepped)
        except Exception:
            _LOG.exception(f"Request failed: {url}")
            return None
        _LOG.debug(
            f"received response. code: {resp.status_code}; "
            f"reason: {resp.reason}; from: {resp.url}"
        )
        return resp

    def esearch(self, term, web_env=None, query_key=None):
        search_params = {
            "db": "pmc",
            "term": term,
            "usehistory": "y",
            "retmode": "json",
            "retmax": 5,
        }
        if web_env is not None and query_key is not None:
            search_params["WebEnv"] = web_env
            search_params["query_key"] = query_key
        resp = self._send_request(
            self._esearch_base_url, params=search_params, verb="POST"
        )
        try:
            search_info = resp.json()["esearchresult"]
        except Exception:
            return {}
        if "ERROR" in search_info:
            return {}
        self.last_search_result = search_info
        return search_info

    def _check_search_info(self, search_info):
        if search_info is not None:
            needed_keys = {"count", "webenv", "querykey"}
            if needed_keys.issubset(search_info.keys()):
                return search_info
            else:
                raise ValueError(
                    f"`search_info` should contain all of {needed_keys}"
                )
        try:
            search_info = self.last_search_result
        except AttributeError:
            raise ValueError(
                "Perform a search before calling `efetch`"
                "or provide `search_info`"
            )
        return search_info

    def efetch(self, search_info=None, n_docs=None, retmax=500):
        search_info = self._check_search_info(search_info)
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
        }
        n_batches = math.ceil(n_docs / retmax)
        n_failures = 0
        while retstart < n_docs:
            _LOG.debug(
                f"getting batch {(retstart // retmax) + 1} / {n_batches}"
            )
            resp = self._send_request(self._efetch_base_url, params=params)
            if resp is None or resp.status_code != 200:
                n_failures += 1
                _LOG.error(f"{n_failures} batches failed to download")
            else:
                yield resp.content
            retstart += retmax
            params["retstart"] = retstart
