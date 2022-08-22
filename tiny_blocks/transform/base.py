from __future__ import annotations
import logging
from typing import Iterator
import pandas as pd
from functools import reduce
from functools import partial
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

    def __rshift__(self, next: "TransformBase" | LoadBase) -> Pipe | Sink:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            from tiny_blocks.pipeline import Pipe

            funcs = [self.get_iter, next.get_iter]
            source = partial(reduce, lambda r, f: f(r), funcs)
            return Pipe(source=source)
        elif isinstance(next, LoadBase):
            from tiny_blocks.pipeline import Sink

            funcs = [self.get_iter, next.exhaust]
            exhaust = partial(reduce, lambda r, f: f(r), funcs)
            return Sink(exhaust=exhaust)
        else:
            raise ValueError("Unsupported Block Type")
