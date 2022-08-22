import logging
from contextlib import contextmanager
from typing import Iterator, List, Literal, Tuple, Dict

import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

__all__ = ["FromSQLTable", "KwargsFromSQLTable"]


logger = logging.getLogger(__name__)


class KwargsFromSQLTable(KwargsExtractBase):
    """
    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    parse_dates: List | Tuple | Dict = None
    columns: List[str] = None
    chunksize: int = 1000


class FromSQLTable(ExtractBase):
    """
    Read SQL Table Block. Defines the read SQL Table Operation.

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.extract import FromSQLTable
        >>>
        >>> str_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> read_sql = FromSQLTable(dsn_conn=str_conn, table_name="test")
        >>> generator = read_sql.get_iter()
        >>> df = pd.concat(generator)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html
    """

    name: Literal["read_sql_table"] = "read_sql_table"
    dsn_conn: str = Field(..., description="Connection string")
    table_name: str = Field(..., description="Table name")
    kwargs: KwargsFromSQLTable = KwargsFromSQLTable()

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
            for chunk in pd.read_sql_table(
                table_name=self.table_name, con=conn, **self.kwargs.to_dict()
            ):
                yield chunk
