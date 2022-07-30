import logging
import pandas as pd
from typing import List, Literal, Iterator
from blocks.etl.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)


__all__ = ["MergeBlock"]

logger = logging.getLogger(__name__)


class KwargsMerge(KwargsTransformBlock):
    """
    Kwargs for block merge
    """

    how: Literal["left", "right", "outer", "inner", "cross"] = "inner"
    on: str | List[str] = None
    left_on: str = None
    right_on: str = None
    left_index: bool = False
    right_index: bool = False
    suffixes: tuple = None
    indicator: Literal["_merge", "left_only", "right_only"] = None
    npartitions: int = None
    shuffle: Literal["disk", "task"] = None


class MergeBlock(TransformBlock):
    """
    Merge Block
    """

    name: Literal["merge"] = "merge"
    kwargs: KwargsMerge = KwargsMerge()

    def process(
        self, *generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Merge
        """
        pass
