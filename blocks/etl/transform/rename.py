import logging
import pandas as pd
from typing import Literal, Iterator
from blocks.etl.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)


__all__ = ["RenameBlock", "KwargsRenameBlock"]


logger = logging.getLogger(__name__)


class KwargsRenameBlock(KwargsTransformBlock):
    """
    Kwargs for FillNa Block
    """

    pass


class RenameBlock(TransformBlock):
    """
    Rename Block
    """

    name: Literal["rename"] = "rename"
    kwargs: KwargsRenameBlock = KwargsRenameBlock()

    def process(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop NaN
        """
        for chunk in generator:
            chunk = chunk.rename(**self.kwargs.to_dict())
            yield chunk
