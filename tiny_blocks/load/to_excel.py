import logging
import shutil
from pathlib import Path
from typing import Iterator, Literal, Dict, Any, Sequence, List
import tempfile

import pandas as pd
from pydantic import Field, AnyUrl
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["ToExcel", "KwargsToExcel"]


logger = logging.getLogger(__name__)


class KwargsToExcel(KwargsLoadBase):
    """
    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.to_excel.html
    """

    sep: str = "|"
    na_rep: str = None
    float_format: str = None
    columns: Sequence = None
    header: bool | List | str = True
    index: bool = False
    index_label: str | Sequence | Literal["False"] = None
    mode: str = None
    storage_options: Dict[str, Any] = None


class ToExcel(LoadBase):
    """
    Write Excel Block. Defines the load to Excel Operation

    Basic example:
        >>> from tiny_blocks.load import ToExcel
        >>> from tiny_blocks.extract import FromExcel
        >>>
        >>> from_excel = FromExcel(path="path/to/source.xlsx")
        >>> to_excel = ToExcel(path="path/to/sink.xlsx")
        >>>
        >>> generator = from_excel.get_iter()
        >>> to_excel.exhaust(generator)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.to_Excel.html
    """

    name: Literal["to_excel"] = "to_excel"
    kwargs: KwargsToExcel = KwargsToExcel()
    path: Path | AnyUrl = Field(..., description="Destination path")

    def exhaust(self, source: Iterator[pd.DataFrame]):
        """
        - Loop the source
        - Send each chunk to Excel
        """
        with tempfile.NamedTemporaryFile(suffix=".xlsx") as file:
            for chunk in source:
                chunk.to_excel(path_or_buf=file, **self.kwargs.to_dict())

            shutil.copy(file.name, str(self.path))
