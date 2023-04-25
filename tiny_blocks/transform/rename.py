import logging
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
    columns: Dict[str, str]
    errors: Literal["ignore", "raise"] = "ignore"

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        for row in source:

            for new_key, old_key in self.columns.items():
                try:
                    row[new_key] = row.pop(old_key)
                except KeyError as error:
                    if self.errors == "raise":
                        raise error

            yield row
