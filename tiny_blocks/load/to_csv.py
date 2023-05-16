import logging
import shutil
import tempfile
import itertools
import csv

from pathlib import Path
from tiny_blocks.base import Row
from pydantic import Field, validator
from typing import Iterator, Literal, List
from tiny_blocks.load.base import LoadBase


__all__ = ["ToCSV"]


logger = logging.getLogger(__name__)


class ToCSV(LoadBase):
    """
    Write CSV Block. Defines the load to CSV Operation

    Basic example:
        >>> from tiny_blocks.load import ToCSV
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path="path/to/source.csv")
        >>> to_csv = ToCSV(path="path/to/sink.csv")
        >>>
        >>> generator = from_csv.get_iter()
        >>> to_csv.exhaust(generator)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.to_csv.html
    """

    name: Literal["to_csv"] = Field(default="to_csv")
    path: Path = Field(..., description="Destination path")
    headers: List[str] = Field(default=None)
    newline: str = Field(default="")

    @validator("path")
    def directory_exists(cls, path):
        if not Path(path).parent.is_dir():
            raise ValueError(f"Folder '{Path(path).parent}' does not exists.")
        return path

    def exhaust(self, source: Iterator[Row]):
        """
        - Loop the source
        - Send each chunk to CSV
        """
        with tempfile.NamedTemporaryFile(suffix=".csv") as file, open(
            file.name, "w", newline=self.newline
        ) as csvfile:

            if self.headers:
                fieldnames = self.headers
            else:
                row = next(source)  # use first row for extracting fieldnames
                fieldnames = list(row.columns())
                itertools.chain([row], source)  # put it back into the source

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in source:  # here we exhaust the rest of the rows
                writer.writerow(row)

            # if no errors, we create the final file.
            shutil.copy(file.name, str(self.path))
