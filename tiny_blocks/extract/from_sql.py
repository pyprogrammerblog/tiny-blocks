import logging
from pydantic import Field
from tiny_blocks.base import Row
from typing import Iterator, Literal
from sqlalchemy import create_engine, text
from tiny_blocks.extract.base import ExtractBase


__all__ = ["FromSQL"]


logger = logging.getLogger(__name__)


class FromSQL(ExtractBase):
    """
    Read SQL Query Block. Defines the read SQL Query Operation

    Basic example:
        >>> from tiny_blocks.extract import FromSQL
        >>>
        >>> dsn_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> read_sql = FromSQL(dsn_conn=dsn_conn, sql="select * from test")
        >>> generator = read_sql.get_iter()
    """

    name: Literal["read_sql"] = "read_sql"
    dsn_conn: str = Field(..., description="Connection string")
    query: str = Field(..., description="SQL Query")

    def get_iter(self) -> Iterator[Row]:

        with create_engine(self.dsn_conn).connect() as conn:  # open connection
            conn.execution_options(stream_results=True)

            for row in conn.execute(text(self.query)):
                yield Row(row)
