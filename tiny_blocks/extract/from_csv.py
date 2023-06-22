from tiny_blocks.extract.base import ExtractBase
from typing import Iterator, Type
from pydantic import BaseModel, ValidationError
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

        if not Path(path).exists():
            raise ValueError(f"Folder '{path}' does not exists")

        self.path = path
        self.row_model = row_model
        self.newline = newline

    def get_iter(self) -> Iterator[BaseModel]:

        collector = []

        with open(self.path, newline=self.newline) as csvfile:
            for row in csv.DictReader(csvfile):
                try:
                    yield self.row_model(**row)
                except ValidationError as errs:
                    if not self.lazy_validation:
                        raise errs
                    collector.append(errs.errors())

            if collector:
                raise ValidationError(errors=collector, model=self.row_model)
