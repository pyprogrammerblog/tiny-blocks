import logging
from typing import List, Literal, Iterator
from tiny_blocks.sources import SQLSource
from pydantic import Field
from tiny_blocks.etl.extract.base import check_types
import pandas as pd
from tiny_blocks.etl.extract.base import (
    KwargsExtractBase,
    ExtractBase,
)

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
    source: SQLSource = Field(..., description="Source Data")
    sql: str = Field(..., description="SQL Query")
    kwargs: KwargsExtractSQLQuery = KwargsExtractSQLQuery()

    @check_types
    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Read SQL
        """
        with self.source.connect() as conn:
            kwargs = self.kwargs.to_dict()
            for chunk in pd.read_sql_query(sql=self.sql, con=conn, **kwargs):
                yield chunk
