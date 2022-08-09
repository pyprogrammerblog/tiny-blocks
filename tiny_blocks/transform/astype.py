import logging
from typing import Dict, Iterator, Literal

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Astype", "KwargsAstype"]


logger = logging.getLogger(__name__)


class KwargsAstype(KwargsTransformBase):
    """
    For more info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
    """

    errors: Literal["raise", "ignore"] = "ignore"


class Astype(TransformBase):
    """
    Astype Block. Defines the casting of types for dataframes or columns"""

    name: Literal["astype"] = "astype"
    dtype: Dict[str, str]
    kwargs: KwargsAstype = KwargsAstype()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Cast types
        """
        for chunk in generator:
            chunk = chunk.astype(dtype=self.dtype, **self.kwargs.to_dict())
            yield chunk
