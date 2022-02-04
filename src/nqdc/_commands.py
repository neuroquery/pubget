import argparse
from pathlib import Path
from typing import Optional, List, Dict

from nqdc._utils import configure_logging
from nqdc._download import DownloadStep, StandaloneDownloadStep
from nqdc._articles import (
    ArticleExtractionStep,
    StandaloneArticleExtractionStep,
)
from nqdc._data_extraction import (
    DataExtractionStep,
    StandaloneDataExtractionStep,
)
from nqdc._vectorization import VectorizationStep, StandaloneVectorizationStep
from nqdc._nimare import NimareStep
from nqdc._labelbuddy import LabelbuddyStep
from nqdc._typing import BaseProcessingStep


def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log_dir",
        type=str,
        default=None,
        help="Directory in which to store log files. Can also be specified by "
        "exporting the NQDC_LOG_DIR environment variable (if both are given "
        "the command-line argument has higher precedence). If not specified, "
        "no log file is written.",
    )
    return parser


def _run_pipeline(
    argv: Optional[List[str]],
    all_steps: List[BaseProcessingStep],
    name: str,
    description: Optional[str] = None,
) -> int:
    parser = _get_parser()
    if description is not None:
        parser.description = description
    for step in all_steps:
        step.edit_argument_parser(parser)
    args = parser.parse_args(argv)
    configure_logging(args.log_dir, "full_pipeline_")
    total_code = 0
    outputs: Dict[str, Path] = {}
    for step in all_steps:
        step_output, code = step.run(args, outputs)
        if step_output is not None:
            outputs[step.name] = step_output
        total_code += code
    return total_code


def download_command(argv: Optional[List[str]] = None) -> int:
    return _run_pipeline(argv, [StandaloneDownloadStep()], "download_")


def extract_articles_command(argv: Optional[List[str]] = None) -> int:
    return _run_pipeline(
        argv, [StandaloneArticleExtractionStep()], "extract_articles_"
    )


def extract_data_command(argv: Optional[List[str]] = None) -> int:
    return _run_pipeline(
        argv, [StandaloneDataExtractionStep()], "extract_data_"
    )


def vectorize_command(argv: Optional[List[str]] = None) -> int:
    return _run_pipeline(argv, [StandaloneVectorizationStep()], "vectorize_")


def full_pipeline_command(argv: Optional[List[str]] = None) -> int:
    all_steps = [
        DownloadStep(),
        ArticleExtractionStep(),
        DataExtractionStep(),
        VectorizationStep(),
        NimareStep(),
        LabelbuddyStep(),
    ]
    description = (
        "Download and process full-text articles from PubMed Central "
        "for the given query. Articles are downloaded and stored in "
        "individual files. Then, their text and stereotactic coordinates "
        "are extracted and stored in csv files. Finally, the text is "
        "vectorized by computing word counts and TFIDF features."
    )

    return _run_pipeline(argv, all_steps, "full_pipeline_", description)
