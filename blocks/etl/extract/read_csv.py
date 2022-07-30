import logging
import pandas as pd
from typing import List, Literal, Union, Iterator
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
    chunksize: int = 1000


class ReadCSVBlock(ExtractBlock):
    """
    ReadCSV Block
    """

    identifier: Literal["read_csv"] = "read_csv"
    source: CSVSource = Field(..., description="Source Data")
    kwargs: KwargsReadCSV = KwargsReadCSV()

    @check_types
    def process(self) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(self.source.path, **self.kwargs.to_dict()):
            yield chunk
