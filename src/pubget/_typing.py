"""Base classes and utilities for typing."""
import argparse
import enum
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from os import PathLike
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

import pandas as pd
from lxml import etree

try:
    from nilearn import maskers
# import only used for type annotations, was called input_data in old nilearn
# versions
except ImportError:  # pragma: nocover
    from nilearn import input_data as maskers


NiftiMasker = maskers.NiftiMasker

PathLikeOrStr = Union[PathLike, str]
# argparse public functions (add_argument_group) return a private type so we
# have to use it here.
# pylint: disable-next=protected-access
ArgparseActions = Union[argparse.ArgumentParser, argparse._ArgumentGroup]


class StopPipeline(Exception):
    """Raised to indicate subsequent steps should not run."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class ExitCode(enum.IntEnum):
    """Exit code for a processing step."""

    COMPLETED = 0
    INCOMPLETE = 1
    ERROR = 2


class Extractor(ABC):
    """Extractors used by the `_data_extraction` module."""

    fields: Tuple[str, ...]
    name: str

    @abstractmethod
    def extract(
        self, article: etree.ElementTree, article_dir: Path
    ) -> Union[Dict[str, Any], pd.DataFrame]:
        """Extract data from an article."""


class Writer(AbstractContextManager):
    """Writers used by the `_data_extraction` module."""

    @abstractmethod
    def write(self, all_data: Mapping[str, Any]) -> None:
        """Write part of data extracted from article to storage."""


class Command:
    """An `pubget` subcommand."""

    name: str
    short_description: str

    @abstractmethod
    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        """Add arguments needed by this command to parser."""

    @abstractmethod
    def run(
        self,
        args: argparse.Namespace,
    ) -> ExitCode:
        """Execute this command. Return exit code."""


class PipelineStep:
    """An individual step in the `pubget` pipeline (`pubget run`)."""

    name: str
    short_description: str

    @abstractmethod
    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        """Add arguments needed by this step to parser."""

    @abstractmethod
    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], ExitCode]:
        """Execute this step. Return resulting directory and exit code."""
