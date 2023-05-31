from tiny_blocks.extract.base import ExtractBase
from typing import Iterator, List, Type
from pydantic import BaseModel
from pathlib import Path

import logging
import csv

logger = logging.getLogger(__name__)


__all__ = ["FromCSV"]


class FromCSV(ExtractBase):
    """
    ReadCSV Block. Defines the read CSV Operation

    Basic example:
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> read_csv = FromCSV(path="/path/to/file.csv")
        >>> generator = read_csv.get_iter()
    """

    def __init__(
        self,
        path: Path,
        row_model: Type[BaseModel],
        newline: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.path = path
        self.row_model = row_model
        self.newline = newline

        if not Path(path).exists():
            raise ValueError(f"Folder '{path}' does not exists")

    def get_iter(self) -> Iterator[BaseModel]:

        with open(self.path, newline=self.newline) as csvfile:
            for row in csv.DictReader(csvfile):
                yield self.row_model(**row)
