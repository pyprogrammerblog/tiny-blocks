import abc
import logging
import pandas as pd
from typing import Iterator
from blocks.etl.base import BaseBlock
from blocks.etl.base import KwargsBase
from blocks.sinks import AnySink


__all__ = ["LoadBlock", "KwargsLoadBlock"]


logger = logging.getLogger(__name__)


class KwargsLoadBlock(KwargsBase):
    pass


class LoadBlock(BaseBlock):
    """
    Load Base Block
    """

    sink: AnySink

    @abc.abstractmethod
    def exhaust(self, generator: Iterator[pd.DataFrame]):
        raise NotImplementedError
