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

__all__ = ["ExtractSQLTable", "KwargsExtractSQLTable"]


logger = logging.getLogger(__name__)


class KwargsExtractSQLTable(KwargsExtractBase):
    """
    Kwargs for ReadSQL
    """

    table_name: str
    index_col: str | List[str] = None
    coerce_float: bool = True
    columns: List[str] = None
    chunksize: int = 1000


class ExtractSQLTable(ExtractBase):
    """
    ReadSQL Block
    """

    name: Literal["read_sql_table"] = "read_sql_table"
    source: SQLSource = Field(..., description="Source Data")
    kwargs: KwargsExtractSQLTable

    @check_types
    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Read SQL
        """
        for chunk in pd.read_sql_table(
            con=self.source.connection_string, **self.kwargs.to_dict()
        ):
            yield chunk
