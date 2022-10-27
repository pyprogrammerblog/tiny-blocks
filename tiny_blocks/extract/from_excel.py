import logging
from typing import Iterator, List, Literal, Dict, Any, Callable

import pandas as pd
from pydantic import Field, FilePath, AnyUrl
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["FromExcel", "KwargsFromExcel"]


class KwargsFromExcel(KwargsExtractBase):
    """
    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
    """

    header: int | List[int] = None
    names: List[str] = None
    index_col: int | List[int] = None
    usecols: str | List[str] | Callable = None
    squeeze: bool = False
    dtype: Dict = None
    converters: Dict = None
    engine: Literal["c", "python"] = None
    true_values: List = None
    false_values: List = None
    chunksize: int = 1000
    storage_options: Dict[str, Any] = None
    skiprows: int = None
    nrows: int = None
    # NA and Missing Data Handling
    na_values: str | List[str] | Dict = None
    keep_default_na: bool = True
    na_filter: bool = True
    verbose: bool = False
    # Datetime Handling
    parse_dates: bool | List[int] | List[str] = None
    date_parser: Callable = None
    # Quoting, Compression, and File Format
    compression: str = "infer"
    thousands: str = None
    decimal: str = "."
    lineterminator: str = None
    quotechar: str = None
    quoting: int = None
    doublequote: bool = True
    escapechar: str = None
    comment: str = None
    encoding: str = None
    encoding_errors: str | None = "strict"
    dialect: str = None
    # Error Handling
    on_bad_lines: Literal["error", "warn", "skip"] | Callable = "skip"
    # others
    delim_whitespace: bool = False
    low_memory: bool = True
    memory_map: bool = False
    float_precision: str = None


class FromExcel(ExtractBase):
    """
    ReadExcel Block. Defines the read Excel Operation

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.extract import FromExcel
        >>>
        >>> read_excel = FromExcel(path="/path/to/file.xlsx")
        >>>
        >>> generator = read_excel.get_iter()
        >>> df = pd.concat(generator)

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
    """

    name: Literal["read_excel"] = "read_excel"
    path: FilePath | AnyUrl = Field(..., description="Path")
    sheet_name: str | int | List[int] = Field(default=None)
    kwargs: KwargsFromExcel = KwargsFromExcel()

    def get_iter(self) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_excel(self.path, **self.kwargs.to_dict()):
            yield chunk
