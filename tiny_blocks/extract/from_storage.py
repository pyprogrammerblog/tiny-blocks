import logging
from typing import Iterator, List, Literal, Union

import pandas as pd
from pydantic import Field, AnyUrl
from pandas.core.generic import StorageOptions
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

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
    storage_options: StorageOptions = None


class ExtractCSV(ExtractBase):
    """
    ReadCSV Block
    """

    name: Literal["read_csv"] = "read_csv"
    path: AnyUrl = Field(..., description="Destination path")
    kwargs: KwargsExtractCSV = KwargsExtractCSV()

    def get_iter(self) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(self.path, **self.kwargs.to_dict()):
            yield chunk
