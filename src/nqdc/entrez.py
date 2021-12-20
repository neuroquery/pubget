import logging
from urllib.parse import urljoin
import math

import requests
from lxml import etree

from nqdc.utils import get_config

_LOG = logging.getLogger(__name__)


class EntrezClient:
    _default_timeout = 10
    _entrez_base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    _esearch_base_url = urljoin(_entrez_base_url, "esearch.fcgi")
    _efetch_base_url = urljoin(_entrez_base_url, "efetch.fcgi")

    def __init__(self):
        self._entrez_id = {"tool": "neuroquery_data_collection"}
        config = get_config()
        if config["email"] != "":
            self._entrez_id["email"] = config["email"]
        self._session = requests.Session()
        self._session.params = self._entrez_id
        self._session.timeout = self._default_timeout

    def _send_request(self, url, params):
        req = requests.Request("GET", url, params=params)
        prepped = self._session.prepare_request(req)
        _LOG.debug(f"sending request: {prepped.url}")
        try:
            resp = self._session.send(prepped)
            _LOG.debug(
                f"received response: {resp.status_code} from {resp.url}"
            )
        except Exception:
            _LOG.exception(f"Request failed: {url}")
            return None
        return resp

    def esearch(self, term, web_env=None, query_key=None):
        search_params = {
            "db": "pmc",
            "term": term,
            "usehistory": "y",
            "retmode": "json",
        }
        if web_env is not None and query_key is not None:
            search_params["WebEnv"] = web_env
            search_params["query_key"] = query_key
        resp = self._send_request(self._esearch_base_url, params=search_params)
        try:
            search_info = resp.json()["esearchresult"]
        except Exception:
            return {}
        if "ERROR" in search_info:
            return {}
        self._last_search_result = search_info
        return search_info

    def efetch(self, search_info=None, n_docs=None, retmax=500):
        if search_info is None:
            search_info = self._last_search_result
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
            try:
                resp = self._send_request(self._efetch_base_url, params=params)
            except Exception:
                n_failures += 1
                _LOG.exception(f"{n_failures} batches failed to download")
            retstart += retmax
            params["retstart"] = retstart
            yield resp.content
