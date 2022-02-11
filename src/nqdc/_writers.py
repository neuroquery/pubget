"""Writers for the `_extract_data` module."""
from __future__ import annotations
import csv
from pathlib import Path
from typing import Tuple, Union, Mapping, Any, Optional, Type, TextIO
from types import TracebackType

import pandas as pd

from nqdc._typing import PathLikeOrStr, BaseWriter, BaseExtractor


class CSVWriter(BaseWriter):
    """Writing extracted data to a csv file."""

    @classmethod
    def from_extractor(
        cls, extractor: BaseExtractor, output_dir: PathLikeOrStr
    ) -> CSVWriter:
        """Initialize the writer based on an extractor's name and fields."""
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True, parents=True)
        return cls(
            extractor.fields,
            extractor.name,
            output_dir.joinpath(f"{extractor.name}.csv"),
        )

    def __init__(
        self, fields: Tuple[str, ...], name: str, csv_path: PathLikeOrStr
    ) -> None:
        self.fields = fields
        self.name = name
        self.csv_path = csv_path
        self._csv_file: Optional[TextIO] = None
        self._writer: Optional[csv.DictWriter] = None

    def __enter__(self) -> None:
        self._csv_file = open(self.csv_path, "w", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._csv_file, self.fields)
        self._writer.writeheader()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        assert self._csv_file is not None  # for mypy
        self._csv_file.close()

    def write(self, all_data: Mapping[str, Any]) -> None:
        assert self._writer is not None
        if all_data.get(self.name) is None:
            return
        data: Union[pd.DataFrame, Mapping[str, Any]] = all_data[self.name]
        if isinstance(data, pd.DataFrame):
            self._writer.writerows(
                data.astype(object)
                .where(data.notnull(), None)
                .to_dict(orient="records")
            )
        else:
            self._writer.writerow(data)
