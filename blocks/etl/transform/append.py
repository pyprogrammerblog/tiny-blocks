import logging
from typing import Literal

from smart_stream.models.blocks.dependencies import TwoInputs
import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)

__all__ = ["AppendBlock"]

logger = logging.getLogger(__name__)


class KwargsAppendBlock(KwargsTransformBlock):
    """
    Kwargs Astype
    """

    ignore_index: bool = False
    verify_integrity: bool = False
    sort: bool = False


class AppendBlock(TransformBlock):
    """
    Append Block
    """

    name: Literal["append"]
    input: TwoInputs
    kwargs: KwargsAppendBlock

    def delayed(self, *df: dd.DataFrame) -> dd.DataFrame:
        """
        Append operation
        """
        raise NotImplementedError

    def dispatch(
        self, df_left: dd.DataFrame, df_right: dd.DataFrame
    ) -> pd.DataFrame:
        """
        Append operation
        """
        kwargs = self.kwargs.dict()
        return df_left.append(df_right, **kwargs)
