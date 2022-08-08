import logging
from typing import Iterator, List, Literal, Sequence, Dict, Any

import pandas as pd
from pydantic import Field, AnyUrl
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["ExtractStorage", "KwargsExtractStorage"]


class KwargsExtractStorage(KwargsExtractBase):
    """
    Kwargs for ReadCSV

    More info about Kwargs:
    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    """

    sep: str = "|"
    header: str | int | List[int] | None = "infer"
    names: List[str] = None
    index_col: int | str | Sequence[str] | Sequence[int] | False = None
    usecols: List[str] = None
    squeze: bool = False
    prefix: str = None
    mangle_dupe_cols: bool = True
    dtype: Dict = None
    converters: Dict = None
    engine: Literal["c", "python"] = None
    true_values: List = None
    false_values: List = None
    chunksize: int = 1000
    storage_options: Dict[str, Any] = None


class ExtractStorage(ExtractBase):
    """ReadCSV Block

    Defines the read CSV Operation.

    Params:
        path: (str). Source URL file.
        kwargs: (dict). Defined in `KwargsExtractStorage` class.
            For more info: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

    Example:
        >>> import pandas as pd
        >>> from pathlib import Path
        >>> from tiny_blocks.extract import ExtractCSV
        >>> extract_csv = ExtractCSV(path="")
        >>> generator = extract_csv.get_iter()
        >>> pd.concat(generator)  # exhaust the generator
        'name,mask,weapon\nRaphael,red,sai\nDonatello,purple,bo staff\n'
    """

    name: Literal["read_csv"] = "read_csv"
    path: AnyUrl = Field(..., description="Destination path")
    kwargs: KwargsExtractStorage = KwargsExtractStorage()

    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Get Iterator
        """
        for chunk in pd.read_csv(self.path, **self.kwargs.to_dict()):
            yield chunk
