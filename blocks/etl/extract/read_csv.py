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


class ReadCSVBlock(ExtractBlock):
    """
    ReadCSV Block
    """

    name: Literal["read_csv"] = "read_csv"
    kwargs: KwargsReadCSV = KwargsReadCSV()
    source: CSVSource = Field(..., description="Source Data")

    @check_types
    def get_iter(self, chunksize: int = 10000) -> Iterator[pd.DataFrame]:
        kwargs = self.kwargs.to_dict() | {"chunksize": chunksize}
        for chunk in pd.read_csv(self.source.path, **kwargs):
            yield chunk

    @check_types
    def process(self) -> pd.DataFrame:
        """
        Read CSV
        """
        kwargs = self.kwargs.to_dict()
        df = pd.read_csv(self.source.path, **kwargs)
        return df
