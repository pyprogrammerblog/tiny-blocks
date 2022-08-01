import logging

import numpy
import pandas as pd
from typing import Literal, Iterator, Dict
from tiny_blocks.etl.transform.base import (
    KwargsTransformBase,
    TransformBase,
)

__all__ = ["Astype", "KwargsAstype"]


logger = logging.getLogger(__name__)


class KwargsAstype(KwargsTransformBase):
    """
    Kwargs Astype
    """

    errors: Literal["raise", "ignore"] = "ignore"


class Astype(TransformBase):
    """
    Astype Block
    """

    name: Literal["astype"]
    dtype: Dict[str, numpy.dtype]
    kwargs: KwargsAstype

    class Config:
        arbitrary_types_allowed = True

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop NaN
        """
        for chunk in generator:
            chunk = chunk.astype(dtype=self.dtype, **self.kwargs.to_dict())
            yield chunk
