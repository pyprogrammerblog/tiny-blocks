import logging
from pydantic import Field
from functools import lru_cache
from typing import Literal, Iterator, Callable
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row


__all__ = ["Apply"]


logger = logging.getLogger(__name__)


class Apply(TransformBase):
    """
    Apply function. Defines block to apply function.

    The method is applied to a single column.
    For different functionality, please rewrite the Block.

    Basic example:
        >>> from tiny_blocks.transform import Apply
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> apply = Apply(func=lambda x: x + 1)
        >>>
        >>> generator = from_csv.get_iter()
        >>> generator = apply.get_iter(generator)
    """

    name: Literal["apply"] = "apply"
    func: Callable = Field(..., description="Callable")

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        for row in source:
            row = self.func(row)
            yield row
