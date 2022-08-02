import logging
from typing import Literal

import pandas as pd
from tiny_blocks.transform.base import (
    KwargsTransformBase,
    TransformBase,
)

__all__ = ["ApplyBlock"]

logger = logging.getLogger(__name__)


class KwargsApplyBlock(KwargsTransformBase):
    """
    Kwargs Apply
    """

    # func: Callable
    axis: int = 1
    args: tuple = None


class ApplyBlock(TransformBase):
    """
    Apply Block
    """

    name: Literal["apply"]
    kwargs: KwargsApplyBlock = KwargsApplyBlock()

    def dispatch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply operation
        """
        kwargs = self.kwargs.to_dict()
        df = df.apply(**kwargs)
        return df
