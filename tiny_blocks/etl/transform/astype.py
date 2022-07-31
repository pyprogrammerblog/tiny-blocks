import logging
from typing import Literal

import dask.dataframe as dd
import pandas as pd
from smart_stream.models.blocks.transform.base import (
    KwargsTransformBlock,
    TransformBlock,
)
from smart_stream.models.blocks.dependencies import OneInput

__all__ = ["AstypeBlock"]

logger = logging.getLogger(__name__)


class KwargsAstype(KwargsTransformBlock):
    """
    Kwargs Astype
    """

    dtype: str
    copy: bool = True
    errors: Literal["raise", "ignore"] = "raise"


class AstypeBlock(TransformBlock):
    """
    Astype Block
    """

    name: Literal["astype"]
    input: OneInput
    kwargs: KwargsAstype

    def delayed(self, block: dd.DataFrame) -> dd.DataFrame:
        """
        Astype operation
        """
        kwargs = self.kwargs.dict()
        block = block.astype(**kwargs)
        return block

    def dispatch(self, block: dd.DataFrame) -> pd.DataFrame:
        """
        Astype operation
        """
        kwargs = self.kwargs.dict()
        block = block.astype(**kwargs)
        return block
