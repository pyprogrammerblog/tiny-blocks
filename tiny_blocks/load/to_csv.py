import logging
from pathlib import Path
from typing import Iterator, Literal, Dict, Any

import pandas as pd
from pydantic import Field, AnyUrl
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["ToCSV", "KwargsToCSV"]


logger = logging.getLogger(__name__)


class KwargsToCSV(KwargsLoadBase):
    """
    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.to_csv.html
    """

    sep: str = "|"
    index: bool = False
    chunksize: int = 1000
    storage_options: Dict[str, Any] = None


class ToCSV(LoadBase):
    """
    Write CSV Block. Defines the load to CSV Operation

    Basic example:
        >>> from tiny_blocks.load import ToCSV
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path="path/to/source.csv")
        >>> to_csv = ToCSV(path="path/to/sink.csv")
        >>> source = from_csv.get_iter()
        >>> to_csv.exhaust(source)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.to_csv.html
    """

    name: Literal["to_csv"] = "to_csv"
    kwargs: KwargsToCSV = KwargsToCSV()
    path: Path | AnyUrl = Field(..., description="Destination path")

    def exhaust(self, source: Iterator[pd.DataFrame]):
        """
        - Loop the source
        - Send each chunk to CSV
        """
        for chunk in source:
            chunk.to_csv(path_or_buf=self.path, **self.kwargs.to_dict())
