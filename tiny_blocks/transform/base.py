from __future__ import annotations
import logging
from typing import Iterator
from tiny_blocks.base import BaseBlock, Row


__all__ = ["TransformBase"]


logger = logging.getLogger(__name__)


class TransformBase(BaseBlock):
    """
    Transform Base Block

    Each transformation Block implements the `get_iter` method.
    This method gets one or multiple iterators and returns
    an Iterator of Rows.
    """

    def get_iter(self, source) -> Iterator[Row]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        transformation block
        """
        raise NotImplementedError
