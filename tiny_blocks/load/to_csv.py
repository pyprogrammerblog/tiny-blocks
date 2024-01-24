import logging
import shutil
import tempfile
import itertools
import csv

from pathlib import Path
from pydantic import BaseModel
from typing import Iterator, Set
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
    """

    path: Path
    columns: Set[str] = None
    newline: str = ""

    def exhaust(self, source: Iterator[BaseModel]):

        with tempfile.NamedTemporaryFile(suffix=".csv") as file, open(
            file.name, "w", newline=self.newline
        ) as csvfile:

            # use first row for extracting fieldnames
            try:
                first_row = next(source)
            except StopIteration:
                raise ValueError("Source is empty. No data to write.")

            fields = set(first_row.model_fields.keys())
            if self.columns and (not_exist := self.columns - fields):
                raise ValueError(f"Not found: {', '.join(not_exist)}")

            # create a dict writer, write the header and the records
            writer = csv.DictWriter(csvfile, fieldnames=self.columns or fields)
            writer.writeheader()
            for row in itertools.chain([first_row], source):
                writer.writerow(row.model_dump())

            # if no errors, we create the final file.
            shutil.copy(file.name, str(self.path))
