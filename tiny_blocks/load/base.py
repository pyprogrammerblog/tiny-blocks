import logging
import abc
from typing import Iterator

import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase

__all__ = ["LoadBase", "KwargsLoadBase"]


logger = logging.getLogger(__name__)


class KwargsLoadBase(KwargsBase):
    pass


class LoadBase(BaseBlock, abc.ABC):
    """
    Load Base Block

    All blocks inheriting the LoadBase class must implement
    the `exhaust` method.
    """

    @abc.abstractmethod
    def exhaust(self, generator: Iterator[pd.DataFrame]):
        """
        Implement the exhaustion of the incoming iterator.

        It is the end of the Pipe.
        """
        raise NotImplementedError
