"""Implementation of the pubget command."""
import argparse
from typing import List, Optional

from pubget import _plugins, _utils
from pubget._articles import ArticleExtractionCommand, ArticleExtractionStep
from pubget._data_extraction import DataExtractionCommand, DataExtractionStep
from pubget._download import DownloadCommand, DownloadStep
from pubget._fit_neuroquery import FitNeuroQueryCommand, FitNeuroQueryStep
from pubget._fit_neurosynth import FitNeuroSynthCommand, FitNeuroSynthStep
from pubget._labelbuddy import LabelbuddyCommand, LabelbuddyStep
from pubget._nimare import NimareCommand, NimareStep
from pubget._pipeline import Pipeline
from pubget._typing import Command
from pubget._vectorization import VectorizationCommand, VectorizationStep
from pubget._vocabulary import (
    VocabularyExtractionCommand,
    VocabularyExtractionStep,
)

_PUBGET_DESCRIPTION = (
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
        "exporting the PUBGET_LOG_DIR environment variable (if both are given "
        "the command-line argument has higher precedence). If not specified, "
        "no log file is written.",
    )
    return parser


def _get_commands() -> List[Command]:
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
    plugin_steps = _plugins.get_plugin_actions()
    pipeline_steps.extend(plugin_steps["pipeline_steps"])
    pipeline = Pipeline(pipeline_steps)
    commands = [
        pipeline,
        DownloadCommand(),
        ArticleExtractionCommand(),
        DataExtractionCommand(),
        VocabularyExtractionCommand(),
        VectorizationCommand(),
        FitNeuroQueryCommand(),
        FitNeuroSynthCommand(),
        LabelbuddyCommand(),
        NimareCommand(),
    ]
    commands.extend(plugin_steps["commands"])
    return commands


def _add_command_subparsers(
    subparsers: argparse._SubParsersAction,
) -> None:
    all_commands = _get_commands()
    for command in all_commands:
        command_parser = subparsers.add_parser(
            command.name,
            parents=[_get_root_parser()],
            help=command.short_description,
        )
        command_parser.set_defaults(run_subcommand=command.run)
        command.edit_argument_parser(command_parser)


def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=_PUBGET_DESCRIPTION)
    version = _utils.get_pubget_version()
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {version}"
    )
    subparsers = parser.add_subparsers(
        title="Commands",
        description="The pubget action to execute. The main one "
        "is 'pubget run', which executes the full pipeline from bulk "
        "download to feature extraction. For help on a specific command "
        "use: 'pubget COMMAND -h' .",
        required=True,
        dest="command",
        metavar="COMMAND",
        help="DESCRIPTION",
    )
    _add_command_subparsers(subparsers)
    return parser


def pubget_command(argv: Optional[List[str]] = None) -> int:
    """Entry point: pubget command-line tool."""
    parser = _get_parser()
    args = parser.parse_args(argv)
    _utils.configure_logging(args.log_dir)
    # int() is for mypy
    return int(args.run_subcommand(args))
