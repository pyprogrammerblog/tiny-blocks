import logging
import pandas as pd
from typing import List, Literal, Union, Iterator
from pydantic import Field
from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.etl.extract.base import (
    KwargsExtractBase,
    ExtractBase,
)

logger = logging.getLogger(__name__)


__all__ = ["ExtractCSV", "KwargsExtractCSV"]


class KwargsExtractCSV(KwargsExtractBase):
    """
    Kwargs for ReadCSV
    """

    sep: str = "|"
    header: Union[str, int, List[int], None] = "infer"
    usecols: List[str] = None
    engine: Literal["c", "python"] = None
    chunksize: int = 1000


class ExtractCSV(ExtractBase):
    """
    ReadCSV Block
    """

    name: Literal["read_csv"] = "read_csv"
    source: CSVSource = Field(..., description="Source Data")
    kwargs: KwargsExtractCSV = KwargsExtractCSV()

    def get_iter(self) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(self.source.path, **self.kwargs.to_dict()):
            yield chunk
