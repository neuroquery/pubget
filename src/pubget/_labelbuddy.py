"""'extract_labelbuddy_data' step: prepare docs for labelbuddy.

https://jeromedockes.github.io/labelbuddy/
"""
import argparse
import json
import logging
import re
from hashlib import md5
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Iterator,
    Mapping,
    Optional,
    TextIO,
    Tuple,
)

import pandas as pd

from pubget import _utils
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    PathLikeOrStr,
    PipelineStep,
)

_LOG = logging.getLogger(__name__)
_STEP_NAME = "extract_labelbuddy_data"
_STEP_DESCRIPTION = (
    "Prepare extracted articles for annotation with labelbuddy."
)
_DEFAULT_BATCH_SIZE = 200
_LOG_PERIOD = 1000
_TEMPLATE = """{authors}
{journal}, {publication_year}

# Title

{title}

# Keywords

{keywords}

# Abstract
{abstract}

# Body
{body}
"""


def _get_inserted_field_positions(
    template: str, fields: Mapping[str, Any]
) -> Dict[str, Tuple[int, int]]:
    """Return the indices in a formatted str where values have been inserted.

    example:
    >>> _get_inserted_field_positions("{a}345{b}7", {"a": "012", "b": "6"})
    {'a': (0, 3), 'b': (6, 7)}
    """
    template_parts = re.split(r"\{([^}]*)\}", template)
    prefixes, field_names = template_parts[::2], template_parts[1::2]
    positions = {}
    start, end = 0, 0
    for pref, name in zip(prefixes, field_names):
        start += len(pref)
        end = start + len(str(fields[name]))
        positions[name] = start, end
        start = end
    return positions


def _format_authors(doc_authors: pd.DataFrame) -> str:
    """Collapse dataframe with one row per author to a single string."""
    return " and ".join(
        f"{row['surname']}, {row['given-names']}"
        for _, row in doc_authors.iterrows()
    )


def _prepare_document(
    doc_text: pd.Series,
    doc_meta: pd.Series,
    doc_authors: pd.DataFrame,
    batch: int,
) -> Dict[str, Any]:
    """Extract information for one article and prepare labelbuddy document."""
    doc_text = doc_text.fillna("")
    doc_info: Dict[str, Any] = {}
    fields = {**doc_text, **doc_meta}
    fields["authors"] = _format_authors(doc_authors)
    doc_info["text"] = _TEMPLATE.format(**fields)
    doc_info["metadata"] = {
        "pmcid": int(doc_meta["pmcid"]),
        "text_md5": md5(doc_info["text"].encode("utf-8")).hexdigest(),
        "field_positions": _get_inserted_field_positions(_TEMPLATE, fields),
        "batch": batch,
    }
    if not pd.isnull(doc_meta["pmid"]):
        doc_info["metadata"]["pmid"] = int(doc_meta["pmid"])
    if not pd.isnull(doc_meta["doi"]):
        doc_info["metadata"]["doi"] = doc_meta["doi"]
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{doc_meta['pmcid']}"
    doc_info["metadata"]["pmc_url"] = url
    doc_info[
        "display_title"
    ] = f'pmcid: <a href="{url}">{doc_meta["pmcid"]}</a>'
    doc_info["list_title"] = f"PMC{doc_meta['pmcid']}  {doc_text['title']}"
    efetch_url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        f"efetch.fcgi?db=pmc&id={doc_meta['pmcid']}"
    )
    doc_info["metadata"]["efetch_url"] = efetch_url
    return doc_info


def _iter_corpus(
    text_fh: TextIO, metadata_fh: TextIO, authors: pd.DataFrame
) -> Generator[Tuple[pd.Series, pd.Series, pd.DataFrame], None, None]:
    """Iterate over articles and provide text, metadata, authors."""
    all_text_chunks = pd.read_csv(text_fh, chunksize=200)
    all_metadata_chunks = pd.read_csv(metadata_fh, chunksize=200)
    n_articles = 0
    for text_chunk, metadata_chunk in zip(
        all_text_chunks, all_metadata_chunks
    ):
        for (_, doc_text), (_, doc_meta) in zip(
            text_chunk.iterrows(), metadata_chunk.iterrows()
        ):
            n_articles += 1
            assert doc_meta["pmcid"] == doc_text["pmcid"]
            doc_authors = authors[authors["pmcid"] == doc_meta["pmcid"]]
            if not n_articles % _LOG_PERIOD:
                _LOG.info(f"Read {n_articles} articles.")
            yield doc_text, doc_meta, doc_authors
    _LOG.info(f"Read {n_articles} articles.")


def _write_labelbuddy_batch(
    all_docs: Iterator[Tuple[pd.Series, pd.Series, pd.DataFrame]],
    batch_nb: int,
    batch_size: Optional[int],
    output_dir: Path,
) -> None:
    """Write labelbuddy documents to jsonl file.

    Writes at most `batch_size` documents (or all documents if `batch_size` is
    `None`) taken from `all_docs` to the appropriate jsonl file. Raises
    `StopIteration` if the `all_docs` iterator runs out.
    """
    batch_file = output_dir.joinpath(f"documents_{batch_nb:0>5}.jsonl")
    n_written = 0
    try:
        with open(batch_file, "w", encoding="utf-8") as out_f, open(
            output_dir.joinpath("batch_info.csv"), "a", encoding="utf-8"
        ) as batch_info_f:
            while batch_size is None or n_written != batch_size:
                doc_info = next(all_docs)
                out_f.write(
                    json.dumps(_prepare_document(*doc_info, batch=batch_nb))
                )
                out_f.write("\n")
                row = (int(doc_info[1]["pmcid"]), batch_file.name, n_written)
                batch_info_f.write(",".join(map(str, row)))
                batch_info_f.write("\n")
                n_written += 1
    finally:
        if batch_file.is_file() and not n_written:
            batch_file.unlink()


def _do_make_labelbuddy_documents(
    extracted_data_dir: Path, output_dir: Path, batch_size: Optional[int]
) -> None:
    """Perform the creation of the labelbuddy jsonl files."""
    text_file = extracted_data_dir.joinpath("text.csv")
    metadata_file = extracted_data_dir.joinpath("metadata.csv")
    authors = pd.read_csv(extracted_data_dir.joinpath("authors.csv"))
    output_dir.joinpath("batch_info.csv").write_text("pmcid,file_name,line\n")
    with open(text_file, encoding="utf-8") as text_fh, open(
        metadata_file, encoding="utf-8"
    ) as metadata_fh:
        all_docs = _iter_corpus(text_fh, metadata_fh, authors)
        batch_nb = 1
        while True:
            try:
                _write_labelbuddy_batch(
                    all_docs, batch_nb, batch_size, output_dir
                )
            except StopIteration:
                return
            else:
                batch_nb += 1


def make_labelbuddy_documents(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    batch_size: Optional[int] = _DEFAULT_BATCH_SIZE,
) -> Tuple[Path, ExitCode]:
    """Prepare articles for annotation with labelbuddy.

    The documents are prepared in JSONL format, with `batch_size` documents in
    each `.jsonl` file. They can thus be imported into labelbuddy with, for
    example: `labelbuddy mydb.labelbuddy --import-docs documents_00001.jsonl`.

    See the
    [labelbuddy documentation](https://jeromedockes.github.io/labelbuddy/)
    for details.

    Parameters
    ----------
    extracted_data_dir
        The directory containing extracted text and metadata. It is a directory
        created by `pubget.extract_data_to_csv`.
    output_dir
        Directory in which to store the created data. If not specified, a
        sibling directory of `extracted_data_dir` whose name ends with
        `_labelbuddyData` is created.
    batch_size
        Number of articles stored in each `.jsonl` file.
        If `None`, put all articles in one file.
    Returns
    -------
    output_dir
        The directory in which the prepared documents are stored.
    exit_code
        COMPLETED if previous steps were complete and INCOMPLETE otherwise.
        Used by the `pubget` command-line interface.
    """
    extracted_data_dir = Path(extracted_data_dir)
    output_dir = _utils.get_output_dir(
        extracted_data_dir, output_dir, "_extractedData", "_labelbuddyData"
    )
    status = _utils.check_steps_status(
        extracted_data_dir, output_dir, __name__
    )
    if not status["need_run"]:
        return output_dir, ExitCode.COMPLETED
    if batch_size is not None and batch_size < 1:
        raise ValueError(f"batch_size must be at least 1, got {batch_size}.")
    _LOG.info(f"Creating labelbuddy data in {output_dir}")
    _do_make_labelbuddy_documents(extracted_data_dir, output_dir, batch_size)
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(output_dir, name=_STEP_NAME, is_complete=is_complete)
    _LOG.info(f"Done creating labelbuddy data in {output_dir}")
    exit_code = ExitCode.COMPLETED if is_complete else ExitCode.INCOMPLETE
    return output_dir, exit_code


def _get_batch_size(args: argparse.Namespace) -> Optional[int]:
    batch_size: Optional[int] = args.labelbuddy_batch_size
    if batch_size is None:
        return _DEFAULT_BATCH_SIZE
    if batch_size == -1:
        return None
    return batch_size


class LabelbuddyStep(PipelineStep):
    """labelbuddy as part of a pipeline (pubget run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "--labelbuddy",
            action="store_true",
            help="Prepare extracted articles for annotation with labelbuddy. "
            "See https://jeromedockes.github.io/labelbuddy/ "
            "for more information.",
        )
        argument_parser.add_argument(
            "--labelbuddy_batch_size",
            type=int,
            default=None,
            help="Number of articles in each jsonl file of documents "
            "prepared for annotation with labelbuddy. "
            f"Default is {_DEFAULT_BATCH_SIZE}. "
            "-1 means put all articles in one file. "
            "This option implies --labelbuddy.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], ExitCode]:
        if not args.labelbuddy and args.labelbuddy_batch_size is None:
            return None, ExitCode.COMPLETED
        return make_labelbuddy_documents(
            previous_steps_output["extract_data"],
            batch_size=_get_batch_size(args),
        )


class LabelbuddyCommand(Command):
    """labelbuddy as a standalone command (pubget extract_labelbuddy_data)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "extracted_data_dir",
            help="Directory containing  extracted data CSV files. "
            "It is a directory created by pubget whose name ends "
            "with 'extractedData'.",
        )
        argument_parser.add_argument(
            "--labelbuddy_batch_size",
            type=int,
            default=None,
            help="Number of articles in each jsonl file of documents "
            "prepared for annotation with labelbuddy. "
            f"Default is {_DEFAULT_BATCH_SIZE}. "
            "-1 means put all articles in one file. ",
        )

    def run(self, args: argparse.Namespace) -> ExitCode:
        return make_labelbuddy_documents(
            args.extracted_data_dir,
            batch_size=_get_batch_size(args),
        )[1]
