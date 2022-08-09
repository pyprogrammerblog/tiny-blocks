import logging
from pathlib import Path
from typing import Iterator, Literal

import pandas as pd
from pydantic import Field
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["ToCSV", "KwargsToCSV"]


logger = logging.getLogger(__name__)


class KwargsToCSV(KwargsLoadBase):
    """
    Kwargs for WriteCSV Block
    """

    sep: str = "|"
    index: bool = False
    chunksize: int = 1000


class ToCSV(LoadBase):
    """
    Write CSV Block

    Defines the load to CSV Operation.

    Params:
        path: (Path). Destination Path.
        kwargs: (dict). Defined in `KwargsLoadCSV` class.
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsToCSV = KwargsToCSV()
    path: Path = Field(..., description="Destination path")

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Exhaust Iterator
        """
        for chunk in generator:
            chunk.to_csv(path_or_buf=self.path, **self.kwargs.to_dict())
