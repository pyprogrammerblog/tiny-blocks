import logging
import pandas as pd
from typing import Literal, Iterator
from blocks.sinks.sql import SQLSink
from blocks.etl.load.base import LoadBlock, KwargsLoadBlock

__all__ = ["WriteSQLBlock", "KwargsWriteSQL"]


logger = logging.getLogger(__name__)


class KwargsWriteSQL(KwargsLoadBlock):
    """
    Kwargs for WriteSQL Block
    """

    chunksize: int = 1000


class WriteSQLBlock(LoadBlock):
    """
    WriteSQL Block
    """

    name: Literal["to_sql"] = "to_sql"
    kwargs: KwargsWriteSQL = KwargsWriteSQL()
    sink: SQLSink

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write SQL Operation.

        Exhaust the generator writing chucks to the Database
        """
        for chunk in generator:
            chunk.to_sql(self.sink.path, **self.kwargs.to_dict())
