import logging
from typing import Literal

import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)
from smart_stream.models.blocks.dependencies import TwoInputs

__all__ = ["AlignBlock"]

logger = logging.getLogger(__name__)


class KwargsAlign(KwargsTransformBlock):
    join: Literal["outer", "inner", "left", "right"] = "outer"
    axis: int = None
    fill_value: int = None


class AlignBlock(TransformBlock):
    """
    Align Block
    """

    name: Literal["align"]
    input: TwoInputs
    kwargs: KwargsAlign

    def delayed(
        self, df_left: dd.DataFrame, df_right: dd.DataFrame
    ) -> dd.DataFrame:
        """
        Align operation
        """
        kwargs = self.kwargs.to_dict()
        df_left = df_left.align(df_right, **kwargs)
        return df_left

    def dispatch(
        self, df_left: pd.DataFrame, df_right: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Align operation
        """
        kwargs = self.kwargs.to_dict()
        df_left = df_left.align(df_right, **kwargs)
        return df_left
