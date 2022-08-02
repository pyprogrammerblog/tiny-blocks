import abc
import logging
from typing import Iterator

import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase

__all__ = ["ExtractBase", "KwargsExtractBase"]


logger = logging.getLogger(__name__)


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBase(BaseBlock):
    """
    Extract Base Block
    """

    @abc.abstractmethod
    def get_iter(self) -> Iterator[pd.DataFrame]:
        raise NotImplementedError