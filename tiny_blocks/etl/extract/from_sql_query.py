import logging
from typing import Dict, List, Literal, Iterator
from sources.sql import SQLSource
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

    sql: str
    index_col: str | List[str] = None
    coerce_float: bool = True
    pare_dates: List[str] | Dict[str:str] = None
    columns: List[str] = None
    chunksize: int = 1000


class ExtractSQLQuery(ExtractBase):
    """
    ReadSQL Block
    """

    name: Literal["read_sql"]
    kwargs: KwargsExtractSQLQuery
    source: SQLSource = Field(..., description="Source Data")

    @check_types
    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Read SQL
        """
        kwargs = self.kwargs.to_dict()
        for chunk in pd.read_sql_query(con=self.source.conn, **kwargs):
            yield chunk
