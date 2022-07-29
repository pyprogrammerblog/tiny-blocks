import logging
import pandas as pd

from typing import Literal, Union, Iterator
from pydantic import Field
from blocks.etl.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)


__all__ = ["FillnaBlock", "KwargsDelayedFillna", "KwargsFillNa"]


logger = logging.getLogger(__name__)


class KwargsFillNa(KwargsTransformBlock):
    """
    Kwargs for FillNa Block
    """

    value: Union[int, str, dict] = Field(default=0)
    method: Literal["backfill", "bfill", "pad", "ffill"] = None
    limit: int = None
    axis: int = None


class KwargsDelayedFillna(KwargsTransformBlock):
    """
    Kwargs Delayed for FillNa Block
    """

    pass


class FillnaBlock(TransformBlock):
    """
    Fillna Block
    """

    name: Literal["fillna"]
    kwargs: KwargsFillNa = KwargsDelayedFillna()
    kwargs_delayed: KwargsDelayedFillna = KwargsDelayedFillna()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        FillNa operation
        """
        for chunk in generator:
            yield self.process(block=chunk)

    def process(self, block: pd.DataFrame) -> pd.DataFrame:
        """
        FillNa operation
        """
        kwargs = self.kwargs.to_dict()
        block = block.fillna(**kwargs)
        return block
