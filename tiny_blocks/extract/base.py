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

    def __rshift__(self, next: TransformBase | LoadBase) -> "ExtractBase":
        """
        The `>>` operator for the pipes library.
        Usually this is not used, and instead the implementation on `Source`
        is used. This will only be used if you do not start a pipe chain with a
        `Source`, but rather with a `Pipe`.
        A `Pipe` may be combined with another `Pipe` to form a `Pipe`.
        A `Pipe` may be combined with a `Sink` to form a `Sink`.
        (Strings are treated as file sinks!)
        """
        if isinstance(next, TransformBase):
            next.get_iter(self.get_iter())
            return next
        elif isinstance(next, LoadBase):
            next.exhaust(generator=self.get_iter())
        else:
            raise TypeError(f"Unexpected Block type {type(next)}")
