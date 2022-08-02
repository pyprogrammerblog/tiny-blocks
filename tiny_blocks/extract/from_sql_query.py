import logging
from contextlib import contextmanager
from typing import Iterator, List, Literal

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
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    columns: List[str] = None
    chunksize: int = 1000


class ExtractSQLQuery(ExtractBase):
    """
    ReadSQL Block
    """

    name: Literal["read_sql"] = "read_sql"
    dsn_conn: str = Field(..., description="Connection string")
    sql: str = Field(..., description="SQL Query")
    kwargs: KwargsExtractSQLQuery = KwargsExtractSQLQuery()

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

    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Read SQL
        """
        with self.connect_db() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in pd.read_sql_query(sql=self.sql, con=conn, **kwargs):
                yield chunk
