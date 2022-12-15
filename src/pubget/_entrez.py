"""Client for the Entrez E-utilities needed for downloading articles."""
import io
import logging
import math
import time
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from urllib.parse import urljoin

import requests
from lxml import etree

from pubget._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)
_EFETCH_DEFAULT_BATCH_SIZE = 500


def _check_response_status(response: requests.Response) -> Tuple[bool, str]:
    """Check that a request was successful."""
    if response.status_code == 200:
        return True, ""
    return False, f"Status code {response.status_code} != 200"


def _check_efetch_response(response: requests.Response) -> Tuple[bool, str]:
    """Check request was successful and content looks like an articleset."""
    check, reason = _check_response_status(response)
    if not check:
        return check, reason
    try:
        parse_events = etree.iterparse(
            io.BytesIO(response.content), events=("start",)
        )
        event, elem = next(parse_events)
        assert event == "start"
        assert elem.tag == "pmc-articleset"
    except Exception:
        return (
            False,
            "response content does not appear to be an XML articleset",
        )
    return True, ""


def _check_esearch_response(response: requests.Response) -> Tuple[bool, str]:
    """Check that esearch request was successful."""
    check, reason = _check_response_status(response)
    if not check:
        return check, reason
    try:
        search_result: Dict[str, str] = response.json()["esearchresult"]
    except Exception:
        return False, "response does not contain JSON with esearchresult key"
    if "ERROR" in search_result:
        return False, f"esearch returned ERROR: {search_result['ERROR']}"
    needed_keys = {"count", "webenv", "querykey"}
    missing_keys = needed_keys.difference(search_result.keys())
    if missing_keys:
        return False, f"keys missing from esearch response: {missing_keys}"
    return True, ""


def _check_epost_response(response: requests.Response) -> Tuple[bool, str]:
    check, reason = _check_response_status(response)
    if not check:
        return check, reason
    try:
        resp_xml = etree.fromstring(response.content)
        webenv = resp_xml.find("WebEnv").text
        assert webenv
        querykey = resp_xml.find("QueryKey").text
        assert querykey
    except Exception:
        return False, "epost response missing webenv or querykey"
    return True, ""


class EntrezClient:
    """Client for esearch and efetch using the pmc database."""

    _default_timeout = 27
    _n_request_attempts = 5
    _delay_before_retry_failed_request = 2.0
    _entrez_base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    _esearch_base_url = urljoin(_entrez_base_url, "esearch.fcgi")
    _efetch_base_url = urljoin(_entrez_base_url, "efetch.fcgi")
    _epost_base_url = urljoin(_entrez_base_url, "epost.fcgi")

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
        else:
            self._request_period = request_period
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

    def _send_one_request(
        self,
        prepped: requests.PreparedRequest,
        response_validator: Callable[[requests.Response], Tuple[bool, str]],
    ) -> Optional[requests.Response]:
        """Send prepared request and check response, return None if failed."""
        self._wait_to_send_request()
        try:
            resp = self._session.send(prepped, timeout=self._default_timeout)
        except Exception:
            _LOG.exception(f"Request failed: {prepped.url}")
            return None
        check, reason = response_validator(resp)
        if not check:
            _LOG.error(
                f"Response failed to validate (reason: {reason}) "
                f"for url {prepped.url}"
            )
            return None
        _LOG.debug(
            f"received response. code: {resp.status_code}; "
            f"reason: {resp.reason}; from: {resp.url}"
        )
        return resp

    def _send_request(
        self,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        data: Optional[Mapping[str, Any]] = None,
        response_validator: Callable[
            [requests.Response], Tuple[bool, str]
        ] = _check_response_status,
    ) -> Union[None, requests.Response]:
        """Try to send a request several times.

        Return the response when a request succeeds; return None if all
        attempts fail.

        """
        req = requests.Request("POST", url, params=params, data=data)
        prepped = self._session.prepare_request(req)
        for attempt in range(self._n_request_attempts):
            _LOG.debug(f"sending request: {prepped.url} (attempt #{attempt})")
            resp = self._send_one_request(prepped, response_validator)
            if resp is not None:
                return resp
            time.sleep(self._delay_before_retry_failed_request)
        return None

    def epost(self, all_pmcids: Sequence[int]) -> Dict[str, str]:
        """Post a list of PMCIDs to the Entrez history server.

        An esearch query is then performed to filter the list of pmcids to keep
        only the open-acccess articles.

        If the function fails, it returns an empty dictionary. Otherwise it
        returns a dictionary with keys "count" "webenv" and "querykey".

        IDs can be posted directly to efetch but by posting them to epost first
        we can then filter by open-access, and the efetch part is handled in
        the same way for queries and id lists.

        """
        # not 'if not all_pmcids' in case someone passes a numpy array
        if len(all_pmcids) == 0:
            _LOG.error("Empty PMCID list.")
            self.n_failures = 1
            return {}
        data = {
            "db": "pmc",
            "id": ",".join(map(str, all_pmcids)),
            **self._entrez_id,
        }
        _LOG.info(f"Posting {len(all_pmcids)} PMCIDs to Entrez.")
        resp = self._send_request(
            self._epost_base_url,
            data=data,
            response_validator=_check_epost_response,
        )
        if resp is None:
            self.n_failures = 1
            return {}
        resp_xml = etree.fromstring(resp.content)
        webenv = resp_xml.find("WebEnv").text
        query_key = resp_xml.find("QueryKey").text
        search_result = self.esearch(webenv=webenv, query_key=query_key)
        if "count" in search_result:
            _LOG.info(
                f"{int(search_result['count'])} / {len(all_pmcids)} articles "
                "are in PMC Open Access."
            )
        return search_result

    def esearch(
        self,
        query: Optional[str] = None,
        webenv: Optional[str] = None,
        query_key: Optional[str] = None,
    ) -> Dict[str, str]:
        """Perform search.

        If webenv and query_key are provided, results will be the intersection
        with existing webenv on the history server -- see Entrez documentation:
        https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch

        Results are always restricted to the open-access subset. Therefore
        `query=None` amounts to searching for all open-access articles
        (possibly within a previous result set if providing `webenv` &
        `query_key`).

        If search fails, returns an empty dictionary. Otherwise returns the
        search results -- keys of interest are "count", "webenv", and
        "querykey".

        """
        term = "open+access[filter]"
        if query is not None:
            term = "&".join((query, term))
        data = {
            "db": "pmc",
            "term": term,
            "usehistory": "y",
            "retmode": "json",
            "retmax": 5,
            **self._entrez_id,
        }
        if webenv is not None and query_key is not None:
            data.update({"WebEnv": webenv, "query_key": query_key})
        resp = self._send_request(
            self._esearch_base_url,
            data=data,
            response_validator=_check_esearch_response,
        )
        if resp is None:
            self.n_failures = 1
            return {}
        search_result: Dict[str, str] = resp.json()["esearchresult"]
        self.last_search_result = search_result
        _LOG.info(f"Search returned {search_result['count']} results")
        return search_result

    def _get_search_result(
        self,
        search_result: Optional[Mapping[str, str]] = None,
    ) -> Optional[Mapping[str, str]]:
        """Return provided search result or result from last search.

        If both are missing, return None.
        """
        if search_result is None:
            search_result = self.last_search_result
        if search_result is None:
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
        retmax: int = _EFETCH_DEFAULT_BATCH_SIZE,
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
        search_result = self._get_search_result(search_result)
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
            self._efetch_base_url,
            data=params,
            response_validator=_check_efetch_response,
        )
        if resp is None:
            self.n_failures += 1
            _LOG.error(f"{self.n_failures} batches failed to download")
        else:
            batch_file.write_bytes(resp.content)
