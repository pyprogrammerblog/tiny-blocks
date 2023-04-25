import logging
from pydantic import Field
from contextlib import contextmanager
from typing import Iterator, Literal
from tiny_blocks.base import Row
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from tiny_blocks.extract.base import ExtractBase

__all__ = ["FromSQLQuery"]


logger = logging.getLogger(__name__)


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
        >>>
        >>> generator = read_sql.get_iter()
        >>> df = pd.concat(generator)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html
    """

    name: Literal["read_sql"] = "read_sql"
    dsn_conn: str = Field(..., description="Connection string")
    query: str = Field(..., description="SQL Query")

    def get_iter(self) -> Iterator[Row]:

        with create_engine(self.dsn_conn).connect() as conn:  # open connection
            conn.execution_options(stream_results=True)

            for row in conn.execute(text(self.query)):
                yield Row(row)
