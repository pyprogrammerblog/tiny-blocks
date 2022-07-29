import logging
from abc import ABC
from typing import Dict, List, Literal, Union, Iterator

import pandas as pd
from pydantic import Field
from blocks.sources.csv import CSVSource
from blocks.etl.extract.base import check_types
from blocks.etl.extract.base import (
    KwargsExtractBlock,
    ExtractBlock,
)

logger = logging.getLogger(__name__)


__all__ = ["ReadCSVBlock"]


class KwargsReadCSV(KwargsExtractBlock):
    """
    Kwargs for ReadCSV
    """

    sep: str = "|"
    header: Union[str, int, List[int], None] = "infer"
    names: List[str] = None
    usecols: List[str] = None
    engine: Literal["c", "pyarrow", "python"] = "pyarrow"


class ReadCSVBlock(ExtractBlock):
    """
    ReadCSV Block
    """
    name: Literal["read_csv"] = "read_csv"
    kwargs: KwargsReadCSV = KwargsReadCSV()
    source: CSVSource = Field(..., description="Source Data")

    def get_iter(self, chunksize: int = 10000) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(self.source.path, chunksize=chunksize, **kwargs):
            yield chunk

    @check_types
    def process(self) -> pd.DataFrame:
        """
        Read CSV
        """
        kwargs = self.kwargs.to_dict()
        df = pd.read_csv(self.source.path, **kwargs)
        return df
