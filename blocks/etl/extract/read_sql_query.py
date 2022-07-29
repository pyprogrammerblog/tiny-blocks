import logging
from typing import Dict, List, Literal, Union
from sources.sql import SQLSource
from pydantic import Field
from blocks.extract.base import check_types
import pandas as pd
from blocks.extract.base import (
    KwargsExtractBlock,
    ExtractBlock,
)

__all__ = ["ReadSQLQueryBlock"]


logger = logging.getLogger(__name__)


class KwargsDelayedReadSQLQuery(KwargsExtractBlock):
    """
    Kwargs for ReadSQL
    """

    blocksize: int = None
    sample: int = 256000
    assume_missing: bool = None
    usecols: List[str] = None
    storage_options: Dict = None
    include_path_column: Union[bool, str] = None


class KwargsReadSQLQuery(KwargsExtractBlock):
    """
    Kwargs for ReadSQL
    """

    index_col: Union[str, List[str]] = None
    coerce_float: bool = True
    pare_dates: Union[List[str], Dict[str:str]] = None
    columns: List[str] = None
    chunksize: int = None


class ReadSQLQueryBlock(ExtractBlock):
    """
    ReadSQL Block
    """

    name: Literal["read_sql"]
    kwargs: KwargsReadSQLQuery = KwargsReadSQLQuery()
    kwargs_delayed: KwargsDelayedReadSQLQuery = KwargsDelayedReadSQLQuery()
    source: SQLSource = Field(..., description="Source Data")

    @check_types
    def delayed(self) -> dd.DataFrame:
        """
        Read SQL
        """
        kwargs = self.kwargs_delayed.to_dict() | self.kwargs.to_dict()
        ddf = dd.read_sql(self.source.conn, **kwargs)
        return ddf

    @check_types
    def dispatch(self) -> pd.DataFrame:
        """
        Read SQL
        """
        kwargs = self.kwargs.to_dict()
        df = pd.read_SQL(self.source.conn, **kwargs)
        return df
