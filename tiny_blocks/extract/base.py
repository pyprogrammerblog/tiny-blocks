import abc
import logging
from typing import Iterator

import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.load.base import LoadBase

__all__ = ["ExtractBase", "KwargsExtractBase"]


logger = logging.getLogger(__name__)


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBase(BaseBlock):
    """
    Extract Base Block
    """

    @abc.abstractmethod
    def get_iter(self) -> Iterator[pd.DataFrame]:
        raise NotImplementedError

    def __rshift__(self, next: TransformBase | LoadBase):
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            from tiny_blocks.pipeline import Pipe

            return Pipe(next.get_iter(self.get_iter()))
        elif isinstance(next, LoadBase):
            next.exhaust(self.get_iter())
        else:
            raise ValueError("Wrong block type")
