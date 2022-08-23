import logging
from typing import Iterator, NoReturn
import pandas as pd
from tiny_blocks.base import BaseBlock
from tiny_blocks.load.base import KwargsBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.load.base import LoadBase
from tiny_blocks.pipeline import FanOut, SmartStream


__all__ = ["ExtractBase", "KwargsExtractBase"]


logger = logging.getLogger(__name__)


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBase(BaseBlock):
    """
    Extract Base Block.

    Each extraction Block implement the `get_iter` method.
    This method return an Iterator of chunked DataFrames
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
    ) -> NoReturn | SmartStream:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            smart_stream = SmartStream()
            smart_stream.graph |= {next.uuid: {self.uuid}}
            smart_stream.blocks.update([self, next])
            smart_stream.current_block = next
            return smart_stream
        elif isinstance(next, LoadBase):
            smart_stream = SmartStream()
            smart_stream.graph |= {next.uuid: {self.uuid}}
            smart_stream.blocks.update([self, next])
            smart_stream.current_block = next
            return smart_stream.exhaust(block=self.uuid)  # finish here
        elif isinstance(next, FanOut):
            smart_stream = SmartStream()
            smart_stream.graph |= {
                sink.uuid: {self.uuid} for sink in next.sinks
            }
            smart_stream.blocks.add(self)
            smart_stream.blocks.update(next.sinks)
            smart_stream.exhaust_multiple(*next.sinks)
            return smart_stream  # the pipe continue
        else:
            raise ValueError("Unsupported Block Type")
