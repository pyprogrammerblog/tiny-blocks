import logging
from pydantic import Field
from typing import Iterator, Literal
from sqlalchemy import create_engine, text
from tiny_blocks.load.base import LoadBase
from tiny_blocks.base import Row


__all__ = ["ToSQL"]


logger = logging.getLogger(__name__)


class ToSQL(LoadBase):
    """
    Load SQL Block. Defines the Loading operation to a SQL Database

    Basic example:
        >>> from tiny_blocks.extract import FromSQL
        >>> from tiny_blocks.load import ToSQL
        >>>
        >>> str_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> from_sql = FromSQL(dsn_conn=str_conn, query="select * from source")
        >>> to_sql = ToSQL(dsn_conn=str_conn, table="sink")
        >>>
        >>> generator = from_sql.get_iter()
        >>> to_sql.exhaust(generator)
    """

    name: Literal["to_sql"] = "to_sql"
    dsn_conn: str = Field(..., description="Connection string")
    table: str = Field(..., description="Destination Table")
    table_flag: str = Field(default=None, description="Flag Table")

    def exhaust(self, source: Iterator[Row]):
        """
        - Connect to DB and yield a transaction
        - Loop the source and send each chunk to SQL
        """
        with create_engine(self.dsn_conn).begin() as conn:  # transaction
            conn.execution_options(stream_results=True, autocommit=True)

            for row in source:  # here we exhaust
                stmt = (
                    f"INSERT INTO {self.table} ({', '.join(row.columns())}) "
                    f"VALUES ({', '.join('%s' * len(row))})"
                )
                conn.execute(text(stmt), row.values())

            if self.table_flag:
                conn.execute(text(f"TRUNCATE TABLE {self.table_flag};"))
