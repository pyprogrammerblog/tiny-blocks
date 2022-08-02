import logging

import pandas as pd
from typing import Literal, Iterator, Dict
from tiny_blocks.transform.base import (
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

    name: Literal["astype"] = "astype"
    dtype: Dict[str, str]
    kwargs: KwargsAstype = KwargsAstype()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop NaN
        """
        for chunk in generator:
            chunk = chunk.astype(dtype=self.dtype, **self.kwargs.to_dict())
            yield chunk
