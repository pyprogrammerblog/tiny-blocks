import logging
import pandas as pd
from typing import Literal, Iterator
from tiny_blocks.etl.transform.base import (
    KwargsTransformBase,
    TransformBase,
)


__all__ = ["Rename", "KwargsRename"]


logger = logging.getLogger(__name__)


class KwargsRename(KwargsTransformBase):
    """
    Kwargs for FillNa Block
    """

    pass


class Rename(TransformBase):
    """
    Rename Block
    """

    name: Literal["rename"] = "rename"
    kwargs: KwargsRename = KwargsRename()

    def process(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Rename
        """
        for chunk in generator:
            chunk = chunk.rename(**self.kwargs.to_dict())
            yield chunk
