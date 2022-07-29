import logging
from typing import Literal

import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)
from smart_stream.models.blocks.dependencies import TwoInputs

__all__ = ["ApplyBlock"]

logger = logging.getLogger(__name__)


class KwargsApplyBlock(KwargsTransformBlock):
    """
    Kwargs Apply
    """

    # func: Callable
    axis: int = 1
    args: tuple = None


class ApplyBlock(TransformBlock):
    """
    Apply Block
    """

    name: Literal["apply"]
    input: TwoInputs
    kwargs: KwargsApplyBlock

    def delayed(self, df: dd.DataFrame) -> dd.DataFrame:
        """
        Apply operation
        """
        kwargs = self.kwargs.to_dict()
        block = df.apply(**kwargs)
        return block

    def dispatch(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply operation
        """
        kwargs = self.kwargs.to_dict()
        df = df.apply(**kwargs)
        return df
