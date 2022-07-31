import logging
import pandas as pd
from typing import Literal, Iterator
from tiny_blocks.sinks import AnySQLSink
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
    sink: AnySQLSink

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write SQL Operation.

        Exhaust the generator writing chucks to the Database
        """
        with self.sink.connect() as conn:
            for chunk in generator:
                chunk.to_sql(con=conn, **self.kwargs.to_dict())
