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
    Read SQL Query Block. Defines the read SQL Query Operation

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.extract import FromSQLQuery
        >>>
        >>> str_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> sql = "select * from test"
        >>> read_sql = FromSQLQuery(dsn_conn=str_conn, sql=sql)
        >>> generator = read_sql.get_iter()
        >>> df = pd.concat(generator)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html
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
        with self.connect_db() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in pd.read_sql_query(sql=self.sql, con=conn, **kwargs):
                yield chunk
