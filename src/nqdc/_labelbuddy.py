"""'extract_labelbuddy_data' step: prepare docs for labelbuddy.

https://jeromedockes.github.io/labelbuddy/
"""
from pathlib import Path
import re
import argparse
import logging
from hashlib import md5
import json
from typing import (
    Mapping,
    Any,
    Dict,
    Tuple,
    Optional,
    Generator,
    TextIO,
    Iterator,
)

import pandas as pd

from nqdc._typing import PathLikeOrStr, BaseProcessingStep, ArgparseActions
from nqdc import _utils

_LOG = logging.getLogger(__name__)
_STEP_NAME = "extract_labelbuddy_data"
_STEP_DESCRIPTION = (
    "Prepare extracted articles for annotation with labelbuddy."
)
_DEFAULT_PART_SIZE = 500
_CHAPTER_SIZE = 20
_LOG_FREQUENCY = 1000
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
    *,
    part: int,
    chapter: int,
    page: int,
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
        "part": part,
        "chapter": chapter,
        "page": page,
    }
    if not pd.isnull(doc_meta["pmid"]):
        doc_info["metadata"]["pmid"] = int(doc_meta["pmid"])
    if not pd.isnull(doc_meta["doi"]):
        doc_info["metadata"]["doi"] = doc_meta["doi"]
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{doc_meta['pmcid']}"
    doc_info["display_title"] = (
        f"pmcid: <a href={url}>{doc_meta['pmcid']}</a>"
        f" — Part {part} Chapter {chapter} Page {page}"
    )
    doc_info["list_title"] = f"{part}.{chapter}.{page: <2} {doc_text['title']}"
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
            if not n_articles % _LOG_FREQUENCY:
                _LOG.info(f"Read {n_articles} articles.")
            yield doc_text, doc_meta, doc_authors
    _LOG.info(f"Read {n_articles} articles.")


def _write_labelbuddy_part(
    all_docs: Iterator[Tuple[pd.Series, pd.Series, pd.DataFrame]],
    part_nb: int,
    part_size: Optional[int],
    output_dir: Path,
) -> None:
    """Write labelbuddy documents to jsonl file.

    Writes at most `part_size` documents (or all documents if `part_size` is
    `None`) taken from `all_docs` to the appropriate jsonl file. Raises
    `StopIteration` if the `all_docs` iterator runs out.
    """
    pagination = {"part": part_nb, "chapter": 1, "page": 1}
    # get the first document so we don't create the file if the generator is
    # exhausted.
    doc_info = next(all_docs)
    with open(
        output_dir.joinpath(f"documents_{part_nb:0>5}.jsonl"),
        "w",
        encoding="utf-8",
    ) as out_f:
        out_f.write(json.dumps(_prepare_document(*doc_info, **pagination)))
        out_f.write("\n")
        pagination["page"] += 1
        n_written = 1
        while part_size is None or n_written != part_size:
            doc_info = next(all_docs)
            out_f.write(json.dumps(_prepare_document(*doc_info, **pagination)))
            out_f.write("\n")
            n_written += 1
            if not pagination["page"] % _CHAPTER_SIZE:
                pagination["chapter"] += 1
                pagination["page"] = 1
            else:
                pagination["page"] += 1


def _do_make_labelbuddy_documents(
    extracted_data_dir: Path, output_dir: Path, part_size: Optional[int]
) -> None:
    """Perform the creation of the labelbuddy jsonl files."""
    text_file = extracted_data_dir.joinpath("text.csv")
    metadata_file = extracted_data_dir.joinpath("metadata.csv")
    authors = pd.read_csv(extracted_data_dir.joinpath("authors.csv"))
    with open(text_file, encoding="utf-8") as text_fh, open(
        metadata_file, encoding="utf-8"
    ) as metadata_fh:
        all_docs = _iter_corpus(text_fh, metadata_fh, authors)
        part_nb = 1
        while True:
            try:
                _write_labelbuddy_part(
                    all_docs, part_nb, part_size, output_dir
                )
            except StopIteration:
                return
            else:
                part_nb += 1


def make_labelbuddy_documents(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    part_size: Optional[int] = _DEFAULT_PART_SIZE,
) -> Tuple[Path, int]:
    """Prepare articles for annotation with labelbuddy.

    The documents are prepared in JSONL format, with `part_size` documents in
    each `.jsonl` file. They can thus be imported into labelbuddy with, for
    example: `labelbuddy mydb.labelbuddy --import-docs documents_00001.jsonl`.

    See the
    [labelbuddy documentation](https://jeromedockes.github.io/labelbuddy/)
    for details.

    Parameters
    ----------
    extracted_data_dir
        The directory containing extracted text and metadata. It is a directory
        created by `nqdc.extract_data_to_csv`.
    output_dir
        Directory in which to store the created data. If not specified, a
        sibling directory of `extracted_data_dir` whose name ends with
        `_labelbuddyData` is created.
    part_size
        Number of articles stored in each `.jsonl` file.
        If `None`, put all articles in one file.
    """
    extracted_data_dir = Path(extracted_data_dir)
    output_dir = _utils.get_output_dir(
        extracted_data_dir, output_dir, "_extractedData", "_labelbuddyData"
    )
    status = _utils.check_steps_status(
        extracted_data_dir, output_dir, __name__
    )
    if not status["need_run"]:
        return output_dir, 0
    if part_size is not None and part_size < 1:
        raise ValueError(f"part_size must be at least 1, got {part_size}.")
    _LOG.info(f"Creating labelbuddy data in {output_dir}")
    _do_make_labelbuddy_documents(extracted_data_dir, output_dir, part_size)
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(output_dir, name=_STEP_NAME, is_complete=is_complete)
    _LOG.info(f"Done creating labelbuddy data in {output_dir}")
    return output_dir, 0


def _get_part_size(args: argparse.Namespace) -> Optional[int]:
    part_size: Optional[int] = args.labelbuddy_part_size
    if part_size is None:
        return _DEFAULT_PART_SIZE
    if part_size == -1:
        return None
    return part_size


class LabelbuddyStep(BaseProcessingStep):
    """labelbuddy as part of a pipeline (nqdc run)."""

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
            "--labelbuddy_part_size",
            type=int,
            default=None,
            help="Number of articles in each jsonl file of documents "
            "prepared for annotation with labelbuddy. "
            f"Default is {_DEFAULT_PART_SIZE}. "
            "-1 means put all articles in one file. "
            "This option implies --labelbuddy.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        if not args.labelbuddy and args.labelbuddy_part_size is None:
            return None, 0
        return make_labelbuddy_documents(
            previous_steps_output["extract_data"],
            part_size=_get_part_size(args),
        )


class StandaloneLabelbuddyStep(BaseProcessingStep):
    """labelbuddy as a standalone command (nqdc extract_labelbuddy_data)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "extracted_data_dir",
            help="Directory containing  extracted data CSV files. "
            "It is a directory created by nqdc whose name ends "
            "with 'extractedData'.",
        )
        argument_parser.add_argument(
            "--labelbuddy_part_size",
            type=int,
            default=None,
            help="Number of articles in each jsonl file of documents "
            "prepared for annotation with labelbuddy. "
            f"Default is {_DEFAULT_PART_SIZE}. "
            "-1 means put all articles in one file. ",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        return make_labelbuddy_documents(
            args.extracted_data_dir,
            part_size=_get_part_size(args),
        )
