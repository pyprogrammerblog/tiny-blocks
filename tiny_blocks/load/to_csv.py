import logging
from pathlib import Path
from typing import Iterator, Literal

import pandas as pd
from pydantic import Field
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["LoadCSV", "KwargsLoadCSV"]


logger = logging.getLogger(__name__)


class KwargsLoadCSV(KwargsLoadBase):
    """
    Kwargs for WriteCSV Block
    """

    sep: str = "|"
    index: bool = False
    chunksize: int = 1000


class LoadCSV(LoadBase):
    """
    Write CSV Block

    Defines the load to CSV Operation.

    Params:
        path (Path). Destination Path.
        kwargs (dict). For more info:
            https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsLoadCSV = KwargsLoadCSV()
    path: Path = Field(..., description="Destination path")

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Exhaust Iterator
        """
        for chunk in generator:
            chunk.to_csv(path_or_buf=self.path, **self.kwargs.to_dict())
