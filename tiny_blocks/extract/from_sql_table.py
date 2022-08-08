import logging
from contextlib import contextmanager
from typing import Iterator, List, Literal, Tuple, Dict

import pandas as pd
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

__all__ = ["ExtractSQLTable", "KwargsExtractSQLTable"]


logger = logging.getLogger(__name__)


class KwargsExtractSQLTable(KwargsExtractBase):
    """
    Kwargs for Read SQL Table
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    parse_dates: List | Tuple | Dict = None
    columns: List[str] = None
    chunksize: int = 1000


class ExtractSQLTable(ExtractBase):
    """
    Read SQL Table Block

    Defines the read SQL Table Operation.

    Params:
        dsn_conn: (str). Source path file.
        table: (str). Table name.
        kwargs: (dict). Defined in `KwargsExtractSQLTable` class.
            For more info: https://pandas.pydata.org/docs/
            reference/api/pandas.read_sql_table.html


    Example:
    >>> import pandas as pd
    >>> from pathlib import Path
    >>> from tiny_blocks.extract import ExtractSQLQuery
    >>> extract_sql = ExtractSQLTable(dsn_conn="psycopg2+postgres...")
    >>> generator = extract_sql.get_iter()
    >>> pd.concat(generator)  # exhaust the generator
    'name,mask,weapon\nRaphael,red,sai\nDonatello,purple,bo staff\n'
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
        Get Iterator
        """
        with self.connect_db() as conn:
            for chunk in pd.read_sql_table(
                table_name=self.table_name, con=conn, **self.kwargs.to_dict()
            ):
                yield chunk
