import logging
from typing import Literal

import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)
from smart_stream.models.blocks.dependencies import OneInput


__all__ = ["KwargsDropNa"]


logger = logging.getLogger(__name__)


class KwargsDropNa(KwargsTransformBlock):
    """
    Kwargs for DropDuplicatesBlock
    """

    ignore_index: bool = None


class DropDuplicatesBlock(TransformBlock):
    """
    Operator DropDuplicatesBlock
    """

    name: Literal["drop_duplicates"]
    kwargs: KwargsDropNa
    input: OneInput

    def delayed(self, block: dd.DataFrame) -> dd.DataFrame:
        """
        Drop_duplicates
        """
        kwargs = self.kwargs.to_dict()
        block = block.dropna(**kwargs)
        return block

    def dispatch(self, block: pd.DataFrame) -> pd.DataFrame:
        """
        Drop_duplicates
        """
        kwargs = self.kwargs.to_dict()
        block = block.dropna(**kwargs)
        return block
