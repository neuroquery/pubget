import argparse
from typing import Optional, List

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
from nqdc._nimare import NimareStep, StandaloneNimareStep
from nqdc._labelbuddy import LabelbuddyStep, StandaloneLabelbuddyStep
from nqdc._pipeline import Pipeline


_NQDC_DESCRIPTION = (
    "Download articles from PubMedCentral and extract "
    "metadata, text, stereotactic coordinates and TFIDF features."
)


def _get_root_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
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


def _add_step_subparsers(
    subparsers: argparse._SubParsersAction,
) -> None:
    all_steps = [
        Pipeline(
            [
                DownloadStep(),
                ArticleExtractionStep(),
                DataExtractionStep(),
                VectorizationStep(),
                LabelbuddyStep(),
                NimareStep(),
            ]
        ),
        StandaloneDownloadStep(),
        StandaloneArticleExtractionStep(),
        StandaloneDataExtractionStep(),
        StandaloneVectorizationStep(),
        StandaloneLabelbuddyStep(),
        StandaloneNimareStep(),
    ]
    for step in all_steps:
        step_parser = subparsers.add_parser(
            step.name,
            parents=[_get_root_parser()],
            help=step.short_description,
        )
        step_parser.set_defaults(run_subcommand=step.run)
        step.edit_argument_parser(step_parser)


def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=_NQDC_DESCRIPTION)
    subparsers = parser.add_subparsers(
        title="Commands",
        description="The nqdc action to execute. The main one "
        "is 'nqdc run', which executes the full pipeline from bulk "
        "download to feature extraction. For help on a specific command "
        "use: 'nqdc COMMAND -h' .",
        required=True,
        dest="command",
        metavar="COMMAND",
        help="DESCRIPTION",
    )
    _add_step_subparsers(subparsers)
    return parser


def nqdc_command(argv: Optional[List[str]] = None) -> int:
    parser = _get_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_dir)
    return int(args.run_subcommand(args, {})[1])
