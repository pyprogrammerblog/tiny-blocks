from pydantic import Field
from typing import Iterator, Literal, List
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row

import itertools
import logging


__all__ = ["DropNone"]


logger = logging.getLogger(__name__)


class DropNone(TransformBase):
    """
    Drop Nan Block. Defines the drop None values functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropNone
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_none = DropNone()
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_none.get_iter(generator)
    """

    name: Literal["drop_none"] = "drop_none"
    subset: List[str] = Field(default_factory=list)

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:
        pass
