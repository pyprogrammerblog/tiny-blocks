import logging
from typing import Dict, List, Literal, Iterator
from sources.sql import SQLSource
from pydantic import Field
from blocks.etl.extract.base import check_types
import pandas as pd
from blocks.etl.extract.base import (
    KwargsExtractBlock,
    ExtractBlock,
)

__all__ = ["ReadSQLQueryBlock"]


logger = logging.getLogger(__name__)


class KwargsReadSQLQuery(KwargsExtractBlock):
    """
    Kwargs for ReadSQL
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    pare_dates: List[str] | Dict[str:str] = None
    columns: List[str] = None
    chunksize: int = None


class ReadSQLQueryBlock(ExtractBlock):
    """
    ReadSQL Block
    """

    name: Literal["read_sql"]
    kwargs: KwargsReadSQLQuery = KwargsReadSQLQuery()
    source: SQLSource = Field(..., description="Source Data")

    @check_types
    def process(self) -> Iterator[pd.DataFrame]:
        """
        Read SQL
        """
        kwargs = self.kwargs.to_dict()
        for chunk in pd.read_sql_query(self.source.conn, **kwargs):
            yield chunk
