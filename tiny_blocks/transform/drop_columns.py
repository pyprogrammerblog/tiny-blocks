import logging
import itertools

from pydantic import Field
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
    columns: List[str] = Field(description="Columns to be dropped")

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        # check the columns exist in the source
        first_row = next(source)
        if missing_columns := set(self.columns) - set(first_row.columns()):
            raise ValueError(f"'{', '.join(missing_columns)}' do not exist.")

        for row in itertools.chain([first_row], source):
            row = {k:v for k,v in row.items() if k in self.columns}
            yield Row(row)
