import abc
import logging
import pandas as pd
from typing import Iterator
from tiny_blocks.etl.base import BaseBlock
from tiny_blocks.etl.base import KwargsBase
from tiny_blocks.sinks import AnySink


__all__ = ["LoadBase", "KwargsLoadBlock"]


logger = logging.getLogger(__name__)


class KwargsLoadBlock(KwargsBase):
    pass


class LoadBase(BaseBlock):
    """
    Load Base Block
    """

    sink: AnySink

    @abc.abstractmethod
    def exhaust(self, generator: Iterator[pd.DataFrame]):
        raise NotImplementedError
