"""'download' step: bulk download from PubMedCentral."""
import logging
import json
import os
from pathlib import Path
import argparse
from typing import Optional, Tuple, Mapping

from nqdc._entrez import EntrezClient
from nqdc import _utils
from nqdc._typing import PathLikeOrStr, BaseProcessingStep, ArgparseActions

_LOG = logging.getLogger(__name__)
_STEP_NAME = "download"
_STEP_DESCRIPTION = "Download articles from PubMed Central."


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
    status = _utils.check_steps_status(None, output_dir, __name__)
    if not status["need_run"]:
        return output_dir, 0
    info_file = output_dir.joinpath("info.json")
    if info_file.is_file():
        info = json.loads(info_file.read_text("utf-8"))
    else:
        output_dir.mkdir(exist_ok=True, parents=True)
        info = {
            "query": query,
            "retmax": retmax,
            "is_complete": False,
            "name": _STEP_NAME,
        }
    _LOG.info(f"Downloading data in {output_dir}")
    client = EntrezClient(api_key=api_key)
    if "search_result" in info and "webenv" in info["search_result"]:
        _LOG.info(
            "Found partial download, resuming download of webenv "
            f"{info['search_result']['webenv']}, "
            f"query key {info['search_result']['querykey']}"
        )
    else:
        _LOG.info("Performing search")
        info["search_result"] = client.esearch(query)
        _utils.write_info(output_dir, **info)
    client.efetch(
        output_dir,
        search_result=info["search_result"],
        n_docs=n_docs,
        retmax=info["retmax"],
    )
    _LOG.info(f"Finished downloading articles in {output_dir}")
    info["is_complete"] = client.n_failures == 0 and (
        n_docs is None or n_docs >= int(info["search_result"]["count"])
    )
    if info["is_complete"]:
        _LOG.info("All articles matching the query have been downloaded")
    else:
        _LOG.warning(
            "Download is incomplete -- not all articles matching "
            "the query have been downloaded"
        )
    _utils.write_info(output_dir, **info)
    return output_dir, int(client.n_failures != 0)


def _get_data_dir_env() -> Optional[str]:
    return os.environ.get("NQDC_DATA_DIR", None)


def _get_data_dir(args: argparse.Namespace) -> Path:
    if args.data_dir is not None:
        return Path(args.data_dir)
    data_dir = _get_data_dir_env()
    if not data_dir:
        raise RuntimeError(
            "The nqdc data directory must be provided either as a command "
            "line argument or through the NQDC_DATA_DIR environment variable."
        )
    return Path(data_dir)


def _get_api_key(args: argparse.Namespace) -> Optional[str]:
    if args.api_key is not None:
        return str(args.api_key)
    return os.environ.get("NQDC_API_KEY", None)


def _get_query(args: argparse.Namespace) -> str:
    if args.query is not None:
        return str(args.query)
    return Path(args.query_file).read_text("utf-8").strip()


def _edit_argument_parser(argument_parser: ArgparseActions) -> None:
    nargs_kw = {"nargs": "?"} if _get_data_dir_env() else {}
    argument_parser.add_argument(
        "data_dir",
        help="Directory in which all nqdc data should be stored. "
        "A subdirectory will be created for the given query. Can also be "
        "provided by exporting the NQDC_DATA_DIR environment variable (if "
        "both are specified the command-line argument has higher precedence).",
        # False positive in this case; see
        # https://github.com/python/mypy/issues/5382. could be avoided by using
        # a TypedDict once we drop support for python 3.7.
        **nargs_kw,  # type: ignore
    )
    group = argument_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-q",
        "--query",
        type=str,
        default=None,
        help="Query with which to search the PubMed Central database. "
        "The query can alternatively be read from a file by using the "
        "query_file parameter.",
    )
    group.add_argument(
        "-f",
        "--query_file",
        type=str,
        default=None,
        help="File in which the query is stored. The query can alternatively "
        "be provided as a string by using the query parameter.",
    )
    argument_parser.add_argument(
        "-n",
        "--n_docs",
        type=int,
        default=None,
        help="Approximate maximum number of articles to download. By default, "
        "all results returned for the search are downloaded. If n_docs is "
        "specified, at most n_docs rounded up to the nearest multiple of 500 "
        "articles will be downloaded.",
    )
    argument_parser.add_argument(
        "--api_key",
        type=str,
        default=None,
        help="API key for the Entrez E-utilities (see "
        "https://www.ncbi.nlm.nih.gov/books/NBK25497/). Can also be provided "
        "by exporting the NQDC_API_KEY environment variable (if both are "
        "specified the command-line argument has higher precedence). If the "
        "API key is provided, it is included in all requests to the Entrez "
        "E-utilities.",
    )


def _download_articles_for_args(args: argparse.Namespace) -> Tuple[Path, int]:
    api_key = _get_api_key(args)
    query = _get_query(args)
    data_dir = _get_data_dir(args)
    download_dir, code = download_articles_for_query(
        query=query,
        data_dir=data_dir,
        n_docs=args.n_docs,
        api_key=api_key,
    )
    return download_dir, code


class DownloadStep(BaseProcessingStep):
    """Download as part of a pipeline (nqdc run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        _edit_argument_parser(argument_parser)

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return _download_articles_for_args(args)


class StandaloneDownloadStep(BaseProcessingStep):
    """Download as a standalone command (nqdc download)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        _edit_argument_parser(argument_parser)
        argument_parser.description = (
            "Download full-text articles from "
            "PubMed Central for the given query."
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return _download_articles_for_args(args)
