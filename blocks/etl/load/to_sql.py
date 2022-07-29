import logging
from typing import Literal

import dask.dataframe as dd
from smart_stream.models.sinks.sql import SQLSink
import pandas as pd
from smart_stream.models.blocks.load.base import LoadBlock, KwargsLoadBlock

__all__ = ["WriteSQLBlock", "KwargsWriteSQL", "KwargsDelayedWriteSQL"]

logger = logging.getLogger(__name__)


class KwargsWriteSQL(KwargsLoadBlock):
    """
    Kwargs for WriteSQL Block
    """

    pass


class KwargsDelayedWriteSQL(KwargsLoadBlock):
    """
    Kwargs for WriteSQL Block
    """

    pass


class WriteSQLBlock(LoadBlock):
    """
    WriteSQL Block
    """

    name: Literal["to_sql"]
    kwargs: KwargsWriteSQL = KwargsWriteSQL()
    kwargs_delayed: KwargsDelayedWriteSQL = KwargsDelayedWriteSQL()
    sink: SQLSink

    def delayed(self, block: dd.DataFrame):
        """
        Write SQL operation
        """
        kwargs = self.kwargs.to_dict() | self.kwargs_delayed.to_dict()
        return block.to_sql(self.sink.conn, **kwargs)

    def dispatch(self, block: pd.DataFrame):
        """
        Write SQL Operation
        """
        kwargs = self.kwargs.to_dict()
        return block.to_sql(self.sink.conn, **kwargs)
