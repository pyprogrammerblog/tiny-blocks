import logging
import itertools

from pydantic import BaseModel
from typing import Iterator, NoReturn, Type
from tiny_blocks.base import BaseBlock
from tiny_blocks.load.base import LoadBase
from tiny_blocks.utils import Pipeline, FanOut
from tiny_blocks.transform.base import TransformBase


__all__ = ["ExtractBase"]


logger = logging.getLogger(__name__)


class ExtractBase(BaseBlock):
    """
    Extract Base Block.

    Each extraction Block implements the `get_iter` method.
    This method returns an Iterator of chunked DataFrames
    """

    row_model: Type[BaseModel]
    lazy_validation: bool = True

    def get_iter(self) -> Iterator[BaseModel]:
        """
        Return an iterator of dataclasses
        """
        raise NotImplementedError

    def __rshift__(
        self, right_block: TransformBase | LoadBase | FanOut
    ) -> NoReturn | Pipeline:

        if isinstance(right_block, TransformBase):
            source = right_block.get_iter(source=self.get_iter())
            return Pipeline(source)
        elif isinstance(right_block, LoadBase):
            return right_block.exhaust(source=self.get_iter())
        elif isinstance(right_block, FanOut):
            # n sources = a source per each load block + 1
            # for the right_block pipe
            n = len(right_block.sinks) + 1
            source, *sources = itertools.tee(self.get_iter(), n)
            right_block.exhaust(*sources)
            return Pipeline(source=source)
        else:
            raise ValueError("Unsupported Block Type")
