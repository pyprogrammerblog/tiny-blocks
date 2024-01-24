import logging
from typing import Iterator
from tiny_blocks.base import BaseBlock
from pydantic import BaseModel


__all__ = ["LoadBase"]


logger = logging.getLogger(__name__)


class LoadBase(BaseBlock):
    """
    Load Base Block

    All blocks inheriting the LoadBase class must implement
    the `exhaust` method.
    """

    def exhaust(self, source: Iterator[BaseModel]):
        """
        Implement the exhaustion of the incoming iterator.

        It is the end of the Pipe.
        """
        raise NotImplementedError
