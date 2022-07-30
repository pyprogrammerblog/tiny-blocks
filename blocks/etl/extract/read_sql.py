import logging
from typing import Dict, List, Literal, Iterator
from blocks.sources.sql import SQLSource
from pydantic import Field
from blocks.etl.extract.base import check_types
import pandas as pd
from blocks.etl.extract.base import (
    KwargsExtractBlock,
    ExtractBlock,
)

__all__ = ["ReadSQLBlock"]


logger = logging.getLogger(__name__)


class KwargsReadSQL(KwargsExtractBlock):
    """
    Kwargs for ReadSQL
    """

    index_col: str | List[str] = None
    coerce_float: bool = True
    pare_dates: List[str] | Dict = None
    columns: List[str] = None
    chunksize: int = 1000


class ReadSQLBlock(ExtractBlock):
    """
    ReadSQL Block
    """

    name: Literal["read_sql"] = "read_sql"
    kwargs: KwargsReadSQL = KwargsReadSQL()
    source: SQLSource = Field(..., description="Source Data")

    @check_types
    def process(self) -> Iterator[pd.DataFrame]:
        """
        Read SQL
        """
        for chunk in pd.read_sql(self.source.conn, **self.kwargs.to_dict()):
            yield chunk
