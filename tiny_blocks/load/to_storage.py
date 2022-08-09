import logging
from typing import Iterator, Literal, Dict, Any

import pandas as pd
from pydantic import Field, AnyUrl
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["ToStorage", "KwargsToStorage"]


class KwargsToStorage(KwargsExtractBase):
    """
    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.to_csv.html
    """

    sep: str = "|"
    engine: Literal["c", "python"] = None
    chunksize: int = 1000
    storage_options: Dict[str, Any] = None


class ToStorage(ExtractBase):
    """
    Write CSV to Storage Block.
    Defines the load to Storage Block Operation
    """

    name: Literal["read_csv"] = "read_csv"
    path: AnyUrl = Field(..., description="Destination path")
    kwargs: KwargsToStorage = KwargsToStorage()

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        for chunk in generator:
            chunk.to_csv(path_or_buf=self.path, **self.kwargs.to_dict())
