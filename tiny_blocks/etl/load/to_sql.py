import logging
import pandas as pd
from typing import Literal, Iterator
from tiny_blocks.sinks.sql import SQLSink
from tiny_blocks.etl.load.base import LoadBase, KwargsLoadBase

__all__ = ["LoadSQL", "KwargsLoadSQL"]


logger = logging.getLogger(__name__)


class KwargsLoadSQL(KwargsLoadBase):
    """
    Kwargs for Load SQL Block
    """

    chunksize: int = 1000


class LoadSQL(LoadBase):
    """
    Load SQL Block
    """

    name: Literal["to_sql"] = "to_sql"
    kwargs: KwargsLoadSQL = KwargsLoadSQL()
    sink: SQLSink

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write SQL Operation.

        Exhaust the generator writing chucks to the Database
        """
        for chunk in generator:
            chunk.to_sql(self.sink.conn, **self.kwargs.to_dict())
