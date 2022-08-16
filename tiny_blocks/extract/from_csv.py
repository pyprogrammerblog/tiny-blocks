import logging
from typing import Iterator, List, Literal, Sequence, Dict, Any

import pandas as pd
from pydantic import Field, FilePath, AnyUrl
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["FromCSV", "KwargsFromCSV"]


class KwargsFromCSV(KwargsExtractBase):
    """
    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    """

    sep: str = "|"
    header: str | int | List[int] | None = "infer"
    names: List[str] = None
    index_col: int | str | Sequence[str] | Sequence[int] = None
    usecols: List[str] = None
    prefix: str = None
    mangle_dupe_cols: bool = True
    dtype: Dict = None
    converters: Dict = None
    engine: Literal["c", "python"] = None
    true_values: List = None
    false_values: List = None
    chunksize: int = 1000
    storage_options: Dict[str, Any] = None


class FromCSV(ExtractBase):
    """
    ReadCSV Block. Defines the read CSV Operation

    Basic Usage:
        >>> import pandas as pd
        >>> from tiny_blocks.extract import FromCSV
        >>> read_csv = FromCSV(path="/path/to/file.csv")
        >>> generator = read_csv.get_iter()
        >>> df = pd.concat(generator)
        >>> assert not df.empty

    See info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    """

    name: Literal["read_csv"] = "read_csv"
    path: FilePath | AnyUrl = Field(..., description="Destination path")
    kwargs: KwargsFromCSV = KwargsFromCSV()

    def get_iter(self) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(self.path, **self.kwargs.to_dict()):
            yield chunk
