import logging
import pandas as pd
from functools import lru_cache
from typing import Literal, Iterator, Callable
from pydantic import Field
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Apply", "KwargsApply"]

logger = logging.getLogger(__name__)


class KwargsApply(KwargsTransformBase):
    """
    Kwargs Apply
    """

    pass


class Apply(TransformBase):
    """
    Apply function. Defines block to apply function.

    The method is applied to a single column.
    For different functionality please rewrite the Block.

    Basic Usage:
        >>> from tiny_blocks.transform import Apply
        >>> from tiny_blocks.extract import FromCSV

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.apply.html
    """

    name: Literal["enrich_from_api"] = "apply"
    apply_to_column: str = Field(..., description="Apply to column")
    set_to_column: str = Field(..., description="Return to column")
    func: Callable = Field(..., description="Callable")
    kwargs: KwargsApply = KwargsApply()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        func = lru_cache(lambda x: self.func(x))

        for chunk in generator:
            chunk[self.set_to_column] = chunk[self.apply_to_column].apply(func)
            yield chunk
