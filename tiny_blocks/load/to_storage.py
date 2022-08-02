import logging
from typing import Iterator, Literal

import pandas as pd
from pandas.core.generic import StorageOptions
from pydantic import Field, AnyUrl
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["ExtractCSV", "KwargsExtractCSV"]


class KwargsExtractCSV(KwargsExtractBase):
    """
    Kwargs for ReadCSV
    """

    sep: str = "|"
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

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write CSV Operation.

        Exhaust the generator writing chucks to the Object Storage
        """
        for chunk in generator:
            chunk.to_csv(path_or_buf=self.path, **self.kwargs.to_dict())
