import logging
from typing import Iterator, Literal, List
from tiny_blocks.base import Row
from tiny_blocks.transform.base import TransformBase


__all__ = ["DropColumns"]


logger = logging.getLogger(__name__)


class DropColumns(TransformBase):
    """
    Drop Columns Block. Defines the drop columns functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropColumns
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = DropColumns(columns=["a"])
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
    """

    name: Literal["drop_columns"] = "drop_columns"
    columns: List[str]
    errors: Literal["ignore", "raise"] = "ignore"

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        for row in source:
            try:
                for key, value in row.items():
                    del row[key]
            except KeyError as error:
                if self.errors == "raise":
                    raise error

            yield row
