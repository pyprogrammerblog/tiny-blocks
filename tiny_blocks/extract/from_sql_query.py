import logging
from contextlib import contextmanager
from typing import Iterator, List, Literal, Dict

import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

__all__ = ["ExtractSQLQuery", "KwargsExtractSQLQuery"]


logger = logging.getLogger(__name__)


class KwargsExtractSQLQuery(KwargsExtractBase):
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


class ExtractSQLQuery(ExtractBase):
    """
    Read SQL Query Block

    Defines the read SQL Query Operation.

    Params:
        dsn_conn: (str). Source path file.
        sql: (str). SQL Query String.
        kwargs: (dict). Defined in `KwargsExtractSQLQuery` class.
            For more info: https://pandas.pydata.org/docs
            /reference/api/pandas.read_sql_query.html

    Example:
        >>> import pandas as pd
        >>> from pathlib import Path
        >>> from tiny_blocks.extract import ExtractSQLQuery
        >>> extract_csv = ExtractSQLQuery(dsn_conn="psycopg2+postgres...")
        >>> generator = extract_csv.get_iter()
        >>> pd.concat(generator)  # exhaust the generator
        'name,mask,weapon\nRaphael,red,sai\nDonatello,purple,bo staff\n'
    """

    name: Literal["read_sql"] = "read_sql"
    dsn_conn: str = Field(..., description="Connection string")
    sql: str = Field(..., description="SQL Query")
    kwargs: KwargsExtractSQLQuery = KwargsExtractSQLQuery()

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
