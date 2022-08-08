import logging
from typing import Dict, Iterator, Literal

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Rename", "KwargsRename"]


logger = logging.getLogger(__name__)


class KwargsRename(KwargsTransformBase):
    """
    Kwargs for Rename Block
    """

    pass


class Rename(TransformBase):
    """
    Rename Block

    Defines Rename columns functionality.

    Params:
        columns: (dict). Renamed columns.
            Key defines old column name and value new column name.
        kwargs: (dict). Kwargs defined at ``KwargsRename``.
    """

    name: Literal["rename"] = "rename"
    kwargs: KwargsRename = KwargsRename()
    columns: Dict[str, str]

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Rename columns
        """
        for chunk in generator:
            chunk = chunk.rename(columns=self.columns, **self.kwargs.to_dict())
            yield chunk
