import logging
from typing import Dict, Iterator, Literal

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Rename", "KwargsRename"]


logger = logging.getLogger(__name__)


class KwargsRename(KwargsTransformBase):
    """
    For more info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
    """

    pass


class Rename(TransformBase):
    """
    Rename Block. Defines Rename columns functionality
    """

    name: Literal["rename"] = "rename"
    kwargs: KwargsRename = KwargsRename()
    columns: Dict[str, str]

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        for chunk in generator:
            chunk = chunk.rename(columns=self.columns, **self.kwargs.to_dict())
            yield chunk
