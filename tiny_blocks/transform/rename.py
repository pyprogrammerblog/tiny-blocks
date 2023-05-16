import logging
import itertools

from pydantic import Field
from typing import Dict, Iterator, Literal
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row

__all__ = ["Rename"]


logger = logging.getLogger(__name__)


class Rename(TransformBase):
    """
    Rename Block. Defines Rename columns functionality

    Basic example:
        >>> from tiny_blocks.transform import Rename
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> sort = Rename(columns={"column_name": "new_column_name"})
        >>>
        >>> generator = from_csv.get_iter()
        >>> generator = sort.get_iter(generator)
    """

    name: Literal["rename"] = "rename"
    columns: Dict[str, str] = Field(description="Mapping dictionary")

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        # check the columns exist in the source
        first_row = next(source)
        if missing_columns := set(self.columns) - set(first_row.columns()):
            raise ValueError(f"'{', '.join(missing_columns)}' do not exists.")

        # rename columns
        for row in itertools.chain([first_row], source):
            for new_key, old_key in self.columns.items():
                row[new_key] = row.pop(old_key)
            yield row
