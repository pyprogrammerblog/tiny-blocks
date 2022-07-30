import logging
from typing import Literal, Iterator

from pydantic import Field
import pandas as pd
from blocks.sinks.csv import CSVSink
from blocks.etl.load.base import LoadBlock, KwargsLoadBlock

__all__ = ["WriteCSVBlock", "KwargsWriteCSV"]


logger = logging.getLogger(__name__)


class KwargsWriteCSV(KwargsLoadBlock):
    """
    Kwargs for WriteCSV Block
    """

    chunksize: int = 1000


class WriteCSVBlock(LoadBlock):
    """
    WriteCSV Block
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsWriteCSV = KwargsWriteCSV()
    sink: CSVSink = Field(..., description="Destination Sink")

    def process(self, generator: Iterator[pd.DataFrame]):
        """
        Write CSV Operation
        """
        for chunk in generator:
            chunk.to_csv(self.sink.path, **self.kwargs.to_dict())
