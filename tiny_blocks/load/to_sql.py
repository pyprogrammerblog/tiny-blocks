import logging
from contextlib import contextmanager
from typing import Iterator, Literal, Dict

import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["ToSQL", "KwargsToSQL"]


logger = logging.getLogger(__name__)


class KwargsToSQL(KwargsLoadBase):
    """
    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    """

    chunksize: int = 1000
    index: bool = False
    if_exists: Literal["fail", "replace", "append"] = "append"
    dtype: Dict = None


class ToSQL(LoadBase):
    """
    Load SQL Block. Defines the Loading operation to a SQL Database

    Basic example:
        >>> from tiny_blocks.extract import FromSQLTable
        >>> from tiny_blocks.load import ToSQL
        >>>
        >>> str_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> from_sql = FromSQLTable(dsn_conn=str_conn, table_name="source")
        >>> to_sql = ToSQL(dsn_conn=str_conn, table_name="sink")
        >>> source = from_sql.get_iter()
        >>> to_sql.exhaust(source)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    """

    name: Literal["to_sql"] = "to_sql"
    dsn_conn: str = Field(..., description="Connection string")
    table_name: str = Field(..., description="Destination Table")
    kwargs: KwargsToSQL = KwargsToSQL()

    @contextmanager
    def connect_db(self) -> Connection:
        """
        Yields a connection to Database defined in `dsn_conn`.

        Parameters set on the connection are:
            - `autocommit` mode set to `True`.
            - Connection mode `stream_results` set as `True`.
        """
        engine = create_engine(self.dsn_conn)
        conn = engine.connect()
        conn.execution_options(stream_results=True, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
            engine.dispose()

    def exhaust(self, source: Iterator[pd.DataFrame]):
        """
        - Connect to DB
        - Loop the source
        - Send each chunk to SQL
        """
        with self.connect_db() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in source:
                chunk.to_sql(name=self.table_name, con=conn, **kwargs)
