import logging
from typing import Iterator, Literal, Union

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Fillna", "KwargsFillNa"]


logger = logging.getLogger(__name__)


class KwargsFillNa(KwargsTransformBase):
    """
    Kwargs for FillNa Block

    For more info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html
    """

    method: Literal["backfill", "bfill", "pad", "ffill"] = None
    limit: int = None
    axis: int = None


class Fillna(TransformBase):
    """
    Fillna Block

    Defines the fill Nan values functionality

    Params:
        value: (int, str, dict). Renamed columns.
            Key defines old column name and value new column name.
        kwargs: (dict). Defined in `KwargsFillNa` class.
    """

    name: Literal["fillna"] = "fillna"
    kwargs: KwargsFillNa = KwargsFillNa()
    value: Union[int, str, dict]

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop NaN
        """
        for chunk in generator:
            chunk = chunk.fillna(value=self.value, **self.kwargs.to_dict())
            yield chunk
