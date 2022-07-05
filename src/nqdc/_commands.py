"""Implementation of the nqdc command."""
import argparse
from typing import Optional, List

from nqdc import _utils
from nqdc._download import DownloadStep, StandaloneDownloadStep
from nqdc._articles import (
    ArticleExtractionStep,
    StandaloneArticleExtractionStep,
)
from nqdc._data_extraction import (
    DataExtractionStep,
    StandaloneDataExtractionStep,
)
from nqdc._vocabulary import (
    VocabularyExtractionStep,
    StandaloneVocabularyExtractionStep,
)
from nqdc._vectorization import VectorizationStep, StandaloneVectorizationStep
from nqdc._fit_neuroquery import FitNeuroQueryStep, StandaloneFitNeuroQueryStep
from nqdc._fit_neurosynth import FitNeuroSynthStep, StandaloneFitNeuroSynthStep
from nqdc._nimare import NimareStep, StandaloneNimareStep
from nqdc._labelbuddy import LabelbuddyStep, StandaloneLabelbuddyStep
from nqdc._pipeline import Pipeline
from nqdc._typing import BaseProcessingStep
from nqdc import _plugins


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


def _get_processing_steps() -> List[BaseProcessingStep]:
    pipeline_steps = [
        DownloadStep(),
        ArticleExtractionStep(),
        DataExtractionStep(),
        VocabularyExtractionStep(),
        VectorizationStep(),
        FitNeuroQueryStep(),
        FitNeuroSynthStep(),
        LabelbuddyStep(),
        NimareStep(),
    ]
    plugin_steps = _plugins.get_plugin_processing_steps()
    pipeline_steps.extend(plugin_steps["pipeline_steps"])
    pipeline = Pipeline(pipeline_steps)
    standalone_steps = [
        pipeline,
        StandaloneDownloadStep(),
        StandaloneArticleExtractionStep(),
        StandaloneDataExtractionStep(),
        StandaloneVocabularyExtractionStep(),
        StandaloneVectorizationStep(),
        StandaloneFitNeuroQueryStep(),
        StandaloneFitNeuroSynthStep(),
        StandaloneLabelbuddyStep(),
        StandaloneNimareStep(),
    ]
    standalone_steps.extend(plugin_steps["standalone_steps"])
    return standalone_steps


def _add_step_subparsers(
    subparsers: argparse._SubParsersAction,
) -> None:
    all_steps = _get_processing_steps()
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
    version = _utils.get_nqdc_version()
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version}"
    )
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
    """Entry point: nqdc command-line tool."""
    parser = _get_parser()
    args = parser.parse_args(argv)
    _utils.configure_logging(args.log_dir)
    return int(args.run_subcommand(args, {})[1])
