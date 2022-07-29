import logging
from typing import List, Literal, Union

import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)
from smart_stream.models.blocks.dependencies import TwoInputs


__all__ = ["MergeBlock"]

logger = logging.getLogger(__name__)


class KwargsMerge(KwargsTransformBlock):
    """
    Kwargs for block merge
    """

    how: Literal["left", "right", "outer", "inner", "cross"] = "inner"
    on: Union[str, List[str]] = None
    left_on: str = None
    right_on: str = None
    left_index: bool = False
    right_index: bool = False
    suffixes: tuple = None
    indicator: Literal["_merge", "left_only", "right_only"] = None
    npartitions: int = None
    shuffle: Literal["disk", "task"] = None

    # TODO: Validate kwargs when merging on index, columns etc.
    #  Some options exclude others, So the interface must be aware.


class MergeBlock(TransformBlock):
    """
    Merge Block
    """

    name: Literal["merge"]
    kwargs: KwargsMerge
    input: TwoInputs

    def delayed(
        self, left_block: dd.DataFrame, right_block: dd.DataFrame
    ) -> dd.DataFrame:
        """
        Merge operation between two DataFrames
        """
        kwargs = self.kwargs.to_dict()
        block = dd.merge(left=left_block, right=right_block, **kwargs)
        return block

    def dispatch(
        self, left_block: pd.DataFrame, right_block: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge operation between two DataFrames
        """
        kwargs = self.kwargs.to_dict()
        block = pd.merge(left=left_block, right=right_block, **kwargs)
        return block
