from typing import Iterator, Literal
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row

import logging


__all__ = ["DropNa"]


logger = logging.getLogger(__name__)


class DropNa(TransformBase):
    """
    Drop Nan Block. Defines the drop None values functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropNa
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = DropNa()
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
    """

    name: Literal["drop_na"] = "drop_na"

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        for row in source:
            for key, value in row.items():
                if value is None:
                    del key
            yield row
