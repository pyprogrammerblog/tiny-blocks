import logging
from typing import Iterator, NoReturn, Union
import pandas as pd
import functools
from tiny_blocks.base import BaseBlock, KwargsBase
from tiny_blocks.load.base import LoadBase
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tiny_blocks.pipeline import Pipe, Sink


__all__ = ["TransformBase", "KwargsTransformBase"]


logger = logging.getLogger(__name__)


class KwargsTransformBase(KwargsBase):
    pass


class TransformBase(BaseBlock):
    """
    Transform Base Block

    Each transformation Block implements the `get_iter` method.
    This method get one or multiple iterators and return
    an Iterator of chunked DataFrames.
    """

    def get_iter(self, source) -> Iterator[pd.DataFrame]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        transformation block
        """
        raise NotImplementedError

    def __rshift__(self, next):
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            from tiny_blocks.pipeline import Pipe

            source = functools.partial(next.get_iter)
            return Pipe(source=source)
        elif isinstance(next, LoadBase):
            from tiny_blocks.pipeline import Sink

            exhaust = functools.partial(next.exhaust)
            return Sink(exhaust=exhaust)
        else:
            raise ValueError("Unsupported Block Type")
