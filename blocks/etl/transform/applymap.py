import logging
from typing import Callable, Literal

import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)
from smart_stream.models.blocks.dependencies import OneInput

__all__ = ["ApplyMapBlock"]

logger = logging.getLogger(__name__)


class KwargsApplyMapBlock(KwargsTransformBlock):
    """
    Kwargs Applymap
    """

    func: Callable
    na_action: str = None


class ApplyMapBlock(TransformBlock):
    """
    Applymap Block
    """

    name: Literal["applymap"]
    input: OneInput
    kwargs: KwargsApplyMapBlock

    def delayed(self, block: dd.DataFrame) -> dd.DataFrame:
        """
        Applymap operation
        """
        kwargs = self.kwargs.to_dict()
        if columns := self.filter_before:
            block[columns] = block[columns].applymap(**kwargs)
        else:
            block = block.applymap(**kwargs)
        return block

    def dispatch(self, df: dd.DataFrame) -> pd.DataFrame:
        """
        Applymap operation
        """
        kwargs = self.kwargs.to_dict()
        if columns := self.filter_before:
            df[columns] = df[columns].applymap(**kwargs)
        else:
            df = df.applymap(**kwargs)
        return df
