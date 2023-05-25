import logging
from typing import Iterator, NoReturn
import itertools
import pandas as pd
from tiny_blocks.base import BaseBlock
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.load.base import LoadBase
from tiny_blocks.utils import Pipeline, FanOut


__all__ = ["ExtractBase"]


logger = logging.getLogger(__name__)


class ExtractBase(BaseBlock):
    """
    Extract Base Block.

    Each extraction Block implements the `get_iter` method.
    This method returns an Iterator of chunked DataFrames
    """

    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        extraction block
        """
        raise NotImplementedError

    def __rshift__(
        self, next: TransformBase | LoadBase | FanOut
    ) -> NoReturn | Pipeline:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.get_iter())
            return Pipeline(source)
        elif isinstance(next, LoadBase):
            return next.exhaust(source=self.get_iter())
        elif isinstance(next, FanOut):
            # n sources = a source per each load block + 1 for the next pipe
            n = len(next.sinks) + 1
            source, *sources = itertools.tee(self.get_iter(), n)
            next.exhaust(*sources)
            return Pipeline(source=source)
        else:
            raise ValueError("Unsupported Block Type")
