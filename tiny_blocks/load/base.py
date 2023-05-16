import logging
from typing import Iterator
from tiny_blocks.base import BaseBlock, Row


__all__ = ["LoadBase"]


logger = logging.getLogger(__name__)


class LoadBase(BaseBlock):
    """
    Load Base Block

    All blocks inheriting the LoadBase class must implement
    the `exhaust` method.
    """

    def exhaust(self, source: Iterator[Row]):
        """
        Implement the exhaustion of the incoming iterator.

        It is the end of the Pipe.
        """
        raise NotImplementedError
