import logging
import pandas as pd

from typing import Literal, Iterator
from tiny_blocks.etl.transform.base import (
    KwargsTransformBase,
    TransformBase,
)


__all__ = ["DropNa", "KwargsDropNa"]


logger = logging.getLogger(__name__)


class KwargsDropNa(KwargsTransformBase):
    """
    Kwargs for DropNa
    """

    ignore_index: bool = None


class DropNa(TransformBase):
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
