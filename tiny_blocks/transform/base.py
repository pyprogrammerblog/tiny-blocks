from __future__ import annotations
import logging
from typing import Iterator
import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase


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
