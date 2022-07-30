import logging
import pandas as pd

from typing import Literal, Iterator
from blocks.etl.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)


__all__ = ["KwargsDropNa"]


logger = logging.getLogger(__name__)


class KwargsDropNa(KwargsTransformBlock):
    """
    Kwargs for DropNa
    """

    ignore_index: bool = None


class DropNaBlock(TransformBlock):
    """
    Operator DropNa
    """

    name: Literal["drop_na"] = "drop_na"
    kwargs: KwargsDropNa

    def process(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop NaN
        """
        for chunk in generator:
            chunk = chunk.dropna(**self.kwargs.to_dict())
            yield chunk
