import logging
import pandas as pd
from typing import Literal, Iterator
from pydantic import Field
from tiny_blocks.sinks.csv import CSVSink
from tiny_blocks.etl.load.base import LoadBase, KwargsLoadBase

__all__ = ["LoadCSV", "KwargsLoadCSV"]


logger = logging.getLogger(__name__)


class KwargsLoadCSV(KwargsLoadBase):
    """
    Kwargs for WriteCSV Block
    """

    chunksize: int = 1000


class LoadCSV(LoadBase):
    """
    WriteCSV Block
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsLoadCSV = KwargsLoadCSV()
    sink: CSVSink = Field(..., description="Destination Sink")

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write CSV Operation.

        Exhaust the generator writing chucks to the CSV
        """
        for chunk in generator:
            chunk.to_csv(path_or_buf=self.sink.path, **self.kwargs.to_dict())
