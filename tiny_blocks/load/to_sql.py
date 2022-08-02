import logging
import pandas as pd
from pydantic import Field
from typing import Literal, Iterator
from sqlalchemy.engine import Connection
from contextlib import contextmanager
from sqlalchemy import create_engine
from tiny_blocks.load.base import LoadBase, KwargsLoadBase

__all__ = ["LoadSQL", "KwargsLoadSQL"]


logger = logging.getLogger(__name__)


class KwargsLoadSQL(KwargsLoadBase):
    """
    Kwargs for Load SQL Block
    """

    chunksize: int = 1000
    index: bool = False
    if_exists: Literal["fail", "replace", "append"] = "append"


class LoadSQL(LoadBase):
    """
    Load SQL Block
    """

    name: Literal["to_sql"] = "to_sql"
    dsn_conn: str = Field(..., description="Connection string")
    table_name: str = Field(..., description="Destination Table")
    kwargs: KwargsLoadSQL = KwargsLoadSQL()

    @contextmanager
    def connect_db(self) -> Connection:
        engine = create_engine(self.dsn_conn)
        conn = engine.connect()
        conn.execution_options(stream_results=True, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
            engine.dispose()

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write SQL Operation.

        Exhaust the generator writing chucks to the Database
        """
        with self.connect_db() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in generator:
                chunk.to_sql(name=self.table_name, con=conn, **kwargs)
