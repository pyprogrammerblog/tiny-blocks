import logging
import pandas as pd
from pydantic import Field
from typing import Literal, Iterator
from tiny_blocks.sinks import SQLSink
from tiny_blocks.etl.load.base import LoadBase, KwargsLoadBase

__all__ = ["LoadSQL", "KwargsLoadSQL"]


logger = logging.getLogger(__name__)


class KwargsLoadSQL(KwargsLoadBase):
    """
    Kwargs for Load SQL Block
    """

    name: str = Field(..., description="Destination table name")
    chunksize: int = 1000
    index: bool = False


class LoadSQL(LoadBase):
    """
    Load SQL Block
    """

    name: Literal["to_sql"] = "to_sql"
    kwargs: KwargsLoadSQL
    sink: SQLSink

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write SQL Operation.

        Exhaust the generator writing chucks to the Database
        """
        for chunk in generator:
            chunk.to_sql(con=self.sink.conn_string, **self.kwargs.to_dict())
