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

    chunksize: int = 1000
    index: bool = False
    if_exists: Literal["fail", "replace", "append"] = "replace"


class LoadSQL(LoadBase):
    """
    Load SQL Block
    """

    name: Literal["to_sql"] = "to_sql"
    sink: SQLSink = Field(..., description="Destination sink")
    table_name: str = Field(..., description="Table name")
    kwargs: KwargsLoadSQL = KwargsLoadSQL()

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write SQL Operation.

        Exhaust the generator writing chucks to the Database
        """
        with self.sink.connect() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in generator:
                chunk.to_sql(name=self.table_name, con=conn, **kwargs)
