import logging
import pandas as pd
from typing import Literal, Iterator, Dict
from tiny_blocks.transform.base import (
    KwargsTransformBase,
    TransformBase,
)


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
    """

    name: Literal["rename"] = "rename"
    kwargs: KwargsRename = KwargsRename()
    columns: Dict[str, str]

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Rename
        """
        for chunk in generator:
            chunk = chunk.rename(columns=self.columns, **self.kwargs.to_dict())
            yield chunk
