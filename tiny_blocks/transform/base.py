import logging
from typing import Iterator, NoReturn
import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase
from tiny_blocks.load.base import LoadBase
from tiny_blocks.pipeline import Sink, FanOut


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

    def __rshift__(
        self, next: "TransformBase" | LoadBase | FanOut
    ) -> NoReturn | Sink:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            sink = Sink(missing=self.uuid)
            sink.graph |= {next.uuid: {self.uuid}}
            sink.blocks.add([self, next])
            return sink
        elif isinstance(next, LoadBase):
            sink = Sink(missing=self.uuid)
            sink.graph |= {next.uuid: {self.uuid}}
            sink.blocks.add([self, next])
            return sink  # finish here
        elif isinstance(next, FanOut):
            sink = Sink(missing=self.uuid)
            sink.graph |= {
                next_sink.uuid: {self.uuid} for next_sink in next.sinks
            }
            sink.blocks.add(self)
            sink.blocks.update(next.sinks)
            return sink  # the pipe continue
        else:
            raise ValueError("Unsupported Block Type")
