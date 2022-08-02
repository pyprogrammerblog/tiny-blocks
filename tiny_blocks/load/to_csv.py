import logging
import pandas as pd
from typing import Literal, Iterator
from pydantic import Field
from pathlib import Path
from tiny_blocks.load.base import LoadBase, KwargsLoadBase

__all__ = ["LoadCSV", "KwargsLoadCSV"]


logger = logging.getLogger(__name__)


class KwargsLoadCSV(KwargsLoadBase):
    """
    Kwargs for WriteCSV Block
    """

    chunksize: int = 1000


class LoadCSV(LoadBase):
    """
    WriteCSV Block
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsLoadCSV = KwargsLoadCSV()
    path: Path = Field(..., description="Destination path")

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Write CSV Operation.

        Exhaust the generator writing chucks to the CSV
        """
        for chunk in generator:
            chunk.to_csv(path_or_buf=self.path, **self.kwargs.to_dict())
