from __future__ import annotations
import csv
from pathlib import Path
from typing import Tuple, Union, Mapping, Any, Optional, Type
from types import TracebackType

import pandas as pd

from nqdc._typing import PathLikeOrStr, BaseWriter


class CSVWriter(BaseWriter):
    @classmethod
    def from_extractor(
        cls, extractor: Any, output_dir: PathLikeOrStr
    ) -> CSVWriter:
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True, parents=True)
        return cls(
            extractor.fields,
            extractor.name,
            output_dir.joinpath(f"{extractor.name}.csv"),
        )

    def __init__(
        self, fields: Tuple[str], name: str, csv_path: PathLikeOrStr
    ) -> None:
        self.fields = fields
        self.name = name
        self.csv_path = csv_path

    def __enter__(self) -> None:
        self.csv_file = open(self.csv_path, "w", encoding="utf-8", newline="")
        self.writer = csv.DictWriter(self.csv_file, self.fields)
        self.writer.writeheader()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.csv_file.close()

    def write(self, all_data: Mapping[str, Any]) -> None:
        if all_data.get(self.name) is None:
            return
        data: Union[pd.DataFrame, Mapping[str, Any]] = all_data[self.name]
        if isinstance(data, pd.DataFrame):
            self.writer.writerows(
                data.astype(object)
                .where(data.notnull(), None)
                .to_dict(orient="records")
            )
        else:
            self.writer.writerow(data)
