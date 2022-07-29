import logging
from typing import Dict, Literal

import pandas as pd
from blocks.sinks.csv import CSVSink
from blocks.etl.load.base import LoadBlock, KwargsLoadBlock

__all__ = ["WriteCSVBlock", "KwargsWriteCSV", "KwargsDelayedWriteCSV"]

logger = logging.getLogger(__name__)


class KwargsDelayedWriteCSV(KwargsLoadBlock):
    """
    Kwargs for WriteCSV Block
    """

    single_file: bool = False
    encoding: str = None
    compression: Literal["gzip", "bz2", "xz"] = None
    compute: bool = True
    storage_options: Dict = None
    header_first_partition_only: bool = None
    compute_kwargs: Dict = None
    sep: str = "|"
    columns: str = None
    index: bool = False


class KwargsWriteCSV(KwargsLoadBlock):
    """
    Kwargs for WriteCSV Block
    """

    compute: bool = False


class WriteCSVBlock(LoadBlock):
    """
    WriteCSV Block
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsWriteCSV = KwargsWriteCSV()
    kwargs_delayed: KwargsDelayedWriteCSV = KwargsDelayedWriteCSV()
    sink: CSVSink

    def delayed(self, df: dd.DataFrame):
        """
        Write CSV operation
        """
        kwargs = self.kwargs.to_dict() | self.kwargs_delayed.to_dict()
        return df.to_csv(self.sink.path, **kwargs)

    def dispatch(self, df: pd.DataFrame):
        """
        Write CSV Operation
        """
        kwargs = self.kwargs.to_dict()
        return df.to_csv(self.sink.path, **kwargs)
