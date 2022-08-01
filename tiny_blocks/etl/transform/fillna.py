import logging
import pandas as pd

from typing import Literal, Union, Iterator
from pydantic import Field
from tiny_blocks.etl.transform.base import (
    KwargsTransformBase,
    TransformBase,
)


__all__ = ["Fillna", "KwargsFillNa"]


logger = logging.getLogger(__name__)


class KwargsFillNa(KwargsTransformBase):
    """
    Kwargs for FillNa Block
    """

    value: Union[int, str, dict] = Field(default=0)
    method: Literal["backfill", "bfill", "pad", "ffill"] = None
    limit: int = None
    axis: int = None


class Fillna(TransformBase):
    """
    Fillna Block
    """

    name: Literal["fillna"] = "fillna"
    kwargs: KwargsFillNa = KwargsFillNa()

    def process(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop NaN
        """
        for chunk in generator:
            chunk = chunk.fillna(**self.kwargs.to_dict())
            yield chunk
