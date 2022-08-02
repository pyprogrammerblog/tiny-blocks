import logging
from contextlib import contextmanager
from typing import Iterator, List, Literal

import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

__all__ = ["ExtractSQLTable", "KwargsExtractSQLTable"]


logger = logging.getLogger(__name__)


class KwargsExtractSQLTable(KwargsExtractBase):
    """
    Kwargs for ReadSQL
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    columns: List[str] = None
    chunksize: int = 1000


class ExtractSQLTable(ExtractBase):
    """
    ReadSQL Block
    """

    name: Literal["read_sql_table"] = "read_sql_table"
    dsn_conn: str = Field(..., description="Connection string")
    table_name: str = Field(..., description="Table name")
    kwargs: KwargsExtractSQLTable = KwargsExtractSQLTable()

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
            for chunk in pd.read_sql_table(
                table_name=self.table_name, con=conn, **self.kwargs.to_dict()
            ):
                yield chunk
