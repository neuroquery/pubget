from pathlib import Path
import re
import argparse
import logging
from hashlib import md5
import json
from typing import Mapping, Any, Dict, Tuple, Optional

import pandas as pd

from nqdc._typing import PathLikeOrStr, BaseProcessingStep
from nqdc import _utils

_LOG = logging.getLogger(__name__)

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


def _format_authors(doc_authors: pd.DataFrame) -> str:
    return " and ".join(
        f"{row['surname']}, {row['given-names']}"
        for _, row in doc_authors.iterrows()
    )


def _prepare_document(
    doc_meta: pd.Series,
    doc_authors: pd.DataFrame,
    doc_text: pd.Series,
) -> Dict[str, Any]:
    doc_text = doc_text.fillna("")
    doc_info: Dict[str, Any] = {}
    doc_info["text"] = _TEMPLATE.format(
        authors=_format_authors(doc_authors), **{**doc_meta, **doc_text}
    )
    doc_info["meta"] = {
        "pmcid": int(doc_meta["pmcid"]),
        "text_md5": md5(doc_info["text"].encode("utf-8")).hexdigest(),
    }
    if not pd.isnull(doc_meta["pmid"]):
        doc_info["meta"]["pmid"] = int(doc_meta["pmid"])
    if not pd.isnull(doc_meta["doi"]):
        doc_info["meta"]["doi"] = doc_meta["doi"]
    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{doc_meta['pmcid']}"
    doc_info["short_title"] = f"pmcid: <a href={url}>{doc_meta['pmcid']}</a>"
    doc_info["long_title"] = f"pmcid: {doc_meta['pmcid']} {doc_text['title']}"
    return doc_info


def _prepare_labelbuddy_batch(
    metadata: pd.DataFrame,
    authors: pd.DataFrame,
    text: pd.DataFrame,
    output_file: Path,
) -> None:
    with open(output_file, "w", encoding="utf-8") as out_f:
        for (_, doc_meta), (_, doc_text) in zip(
            metadata.iterrows(), text.iterrows()
        ):
            assert doc_meta["pmcid"] == doc_text["pmcid"]
            doc_authors = authors[authors["pmcid"] == doc_meta["pmcid"]]
            out_f.write(
                json.dumps(_prepare_document(doc_meta, doc_authors, doc_text))
            )
            out_f.write("\n")


def _get_output_dir(
    extracted_data_dir: Path, output_dir: Optional[PathLikeOrStr]
) -> Path:
    if output_dir is None:
        output_dir_name = re.sub(
            r"^(.*?)(_extractedData)?$",
            r"\1_labelbuddyData",
            extracted_data_dir.name,
        )
        output_dir = extracted_data_dir.with_name(output_dir_name)
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    return output_dir


def prepare_labelbuddy_documents(
    extracted_data_dir: PathLikeOrStr,
    output_dir: Optional[PathLikeOrStr] = None,
    batch_size: int = 200,
) -> Tuple[Path, int]:
    """Prepare articles for annotation with labelbuddy.

    The documents are prepared in JSONL format, with `batch_size` documents in
    each `.jsonl` file. They can thus be imported into labelbuddy with, for
    example: `labelbuddy mydb.labelbuddy --import-docs documents_00000.jsonl`.

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
    batch_size
        Number of articles stored in each `.jsonl` file.
    """
    extracted_data_dir = Path(extracted_data_dir)
    output_dir = _get_output_dir(extracted_data_dir, output_dir)
    status = _utils.check_steps_status(
        extracted_data_dir, output_dir, __name__
    )
    if not status["need_run"]:
        return output_dir, 0
    _LOG.info(f"Creating labelbuddy data in {output_dir}")
    authors = pd.read_csv(extracted_data_dir.joinpath("authors.csv"))
    all_metadata = pd.read_csv(
        extracted_data_dir.joinpath("metadata.csv"),
        chunksize=batch_size,
    )
    all_text = pd.read_csv(
        extracted_data_dir.joinpath("text.csv"), chunksize=batch_size
    )
    for i, (metadata, text) in enumerate(zip(all_metadata, all_text)):
        output_file = output_dir.joinpath(f"documents_{i:0>5}.jsonl")
        _prepare_labelbuddy_batch(metadata, authors, text, output_file)
    is_complete = bool(status["previous_step_complete"])
    _utils.write_info(
        output_dir, name="labelbuddy_data_creation", is_complete=is_complete
    )
    _LOG.info(f"Done creating labelbuddy data in {output_dir}")
    return output_dir, 0


class LabelbuddyStep(BaseProcessingStep):
    name = "labelbuddy"

    def edit_argument_parser(
        self, argument_parser: argparse.ArgumentParser
    ) -> None:
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
            help="Number of articles in each batch of documents "
            "prepared for annotation with labelbuddy.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        if not args.labelbuddy and args.labelbuddy_batch_size is None:
            return None, 0
        return prepare_labelbuddy_documents(
            previous_steps_output["data_extraction"],
            batch_size=(args.labelbuddy_batch_size or 200),
        )
