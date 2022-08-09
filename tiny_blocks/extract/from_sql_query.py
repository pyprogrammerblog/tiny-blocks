import logging
from contextlib import contextmanager
from typing import Iterator, List, Literal, Dict

import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

__all__ = ["FromSQLQuery", "KwargsFromSQLQuery"]


logger = logging.getLogger(__name__)


class KwargsFromSQLQuery(KwargsExtractBase):
    """
    Kwargs for ReadSQL

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    columns: List[str] = None
    parse_dates: List | Dict = None
    dtype: Dict = None
    chunksize: int = 1000


class FromSQLQuery(ExtractBase):
    """
    Read SQL Query Block

    Defines the read SQL Query Operation.

    Params:
        dsn_conn: (str). Source path file.
        sql: (str). SQL Query String.
        kwargs: (dict). Defined in `KwargsExtractSQLQuery` class.
    """

    name: Literal["read_sql"] = "read_sql"
    dsn_conn: str = Field(..., description="Connection string")
    sql: str = Field(..., description="SQL Query")
    kwargs: KwargsFromSQLQuery = KwargsFromSQLQuery()

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

    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Get Iterator
        """
        with self.connect_db() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in pd.read_sql_query(sql=self.sql, con=conn, **kwargs):
                yield chunk
