import abc
import logging
from typing import Iterator

import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase

__all__ = ["LoadBase", "KwargsLoadBase"]


logger = logging.getLogger(__name__)


class KwargsLoadBase(KwargsBase):
    pass


class LoadBase(BaseBlock):
    """
    Load Base Block
    """

    @abc.abstractmethod
    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Implement the exhaustion the iterator

        The `chunksize` is defined as kwargs in each
        loading block
        """
        raise NotImplementedError
