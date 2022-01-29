from os import PathLike
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Union, Dict, Any, Tuple, Mapping

from lxml import etree
import pandas as pd

PathLikeOrStr = Union[PathLike, str]


class BaseExtractor(ABC):
    @property
    @abstractmethod
    def fields(self) -> Tuple[str, ...]:
        """Dict keys or DataFrame columns produced by this extractor."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name for this extractor."""

    @abstractmethod
    def extract(
        self, article: etree.ElementTree
    ) -> Union[Dict[str, Any], pd.DataFrame]:
        """Extract data from an article"""


class BaseWriter(AbstractContextManager):
    @abstractmethod
    def write(self, all_data: Mapping[str, Any]) -> None:
        """Write part of data extracted from article to storage."""
