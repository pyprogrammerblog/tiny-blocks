import logging
from typing import Iterator, Literal, Union

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Fillna", "KwargsFillNa"]


logger = logging.getLogger(__name__)


class KwargsFillNa(KwargsTransformBase):
    """
    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html
    """

    method: Literal["backfill", "bfill", "pad", "ffill"] = None
    limit: int = None
    axis: int = None


class Fillna(TransformBase):
    """
    Fill Nan Block. Defines the fill Nan values functionality

    Basic Usage:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.extract import FromCSV
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> fill_na = Fillna(value="Hola Mundo")
        >>> generator = extract_csv.get_iter()
        >>> generator = fill_na.get_iter(generator)
        >>> df = pd.concat(generator)
        >>> assert not df.empty

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html
    """

    name: Literal["fillna"] = "fillna"
    kwargs: KwargsFillNa = KwargsFillNa()
    value: Union[int, str, dict]

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        for chunk in generator:
            chunk = chunk.fillna(value=self.value, **self.kwargs.to_dict())
            yield chunk
