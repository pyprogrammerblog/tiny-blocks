from tiny_blocks.extract.base import ExtractBase
from typing import Iterator, Literal
from pydantic import BaseModel, ValidationError, FilePath
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
        >>> next(generator)
    """

    name: Literal["from_csv"] = "from_csv"
    path: FilePath
    separator: Literal[",", "|", ";"] = ";"
    newline: str = ""

    def get_iter(self) -> Iterator[BaseModel]:

        errors = []

        with open(str(self.path), newline=self.newline) as csvfile:
            for row in csv.DictReader(csvfile):
                try:
                    yield self.row_model(**row)
                except ValidationError as errs:
                    if not self.lazy_validation:
                        raise errs
                    errors.extend(errs.errors())

            if errors:
                raise ValidationError(errors, self.row_model)
