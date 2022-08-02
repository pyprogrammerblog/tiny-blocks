import abc
import logging
from typing import Iterator

import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase
from tiny_blocks.load.base import LoadBase

__all__ = ["TransformBase", "KwargsTransformBase"]


logger = logging.getLogger(__name__)


class KwargsTransformBase(KwargsBase):
    pass


class TransformBase(BaseBlock):
    """
    Extract Base Block
    """

    @abc.abstractmethod
    def get_iter(
        self, *generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        raise NotImplementedError

    def __rshift__(
        self, next: "TransformBase" | LoadBase
    ) -> "TransformBase" | LoadBase:
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
            return next.get_iter(self.get_iter())
        elif isinstance(next, LoadBase):
            next.exhaust(generator=self.get_iter())
        else:
            raise TypeError(f"Unexpected Block type {type(next)}")
