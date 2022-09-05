import logging
import shutil
from pathlib import Path
from typing import Iterator, Literal, Dict, Any, Sequence, List
import tempfile

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
    na_rep: str = None
    float_format: str = None
    columns: Sequence = None
    header: bool | List | str = True
    index: bool = False
    index_label: str | Sequence | Literal["False"] = None
    mode: str = None
    encoding: str = None
    compression: str | Dict = "infer"
    quoting: str = None
    quotechar: str = None
    line_terminator: str = None
    chunksize: int = 1000
    date_format: str = None
    doublequote: bool = None
    escapechar: str = None
    decimal: str = None
    errors: str = None
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
        >>>
        >>> generator = from_csv.get_iter()
        >>> to_csv.exhaust(generator)

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
        with tempfile.NamedTemporaryFile(suffix=".csv") as file:
            for chunk in source:
                chunk.to_csv(path_or_buf=file, **self.kwargs.to_dict())

            shutil.copy(file.name, str(self.path))
