import logging
from typing import Iterator, Literal

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["DropNa", "KwargsDropNa"]


logger = logging.getLogger(__name__)


class KwargsDropNa(KwargsTransformBase):
    """
    Kwargs for DropNa
    """

    ignore_index: bool = None


class DropNa(TransformBase):
    """
    Drop Nan Block. Defines the drop None values functionality
    """

    name: Literal["drop_na"] = "drop_na"
    kwargs: KwargsDropNa = KwargsDropNa()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        for chunk in generator:
            chunk = chunk.dropna(**self.kwargs.to_dict())
            yield chunk
