import logging
import pandas as pd
from functools import lru_cache
from typing import Literal, Iterator
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
    Apply function. Defines block to apply function
    """

    name: Literal["enrich_from_api"] = "enrich_from_api"
    apply_to: str = Field(description="Apply to column")
    return_to: str = Field(description="Return to column")
    kwargs: KwargsApply = KwargsApply()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        func = lru_cache(lambda x: self.func(x))

        for chunk in generator:
            chunk[self.return_to] = chunk[self.apply_to].apply(func)
            yield chunk

    def func(self, value):
        raise NotImplementedError
