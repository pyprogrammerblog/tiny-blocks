import abc
import logging
import pandas as pd
from typing import Iterator
from tiny_blocks.etl.base import BaseBlock
from tiny_blocks.etl.base import KwargsBase
from tiny_blocks.sinks import Sink


__all__ = ["LoadBase", "KwargsLoadBase"]


logger = logging.getLogger(__name__)


class KwargsLoadBase(KwargsBase):
    pass


class LoadBase(BaseBlock):
    """
    Load Base Block
    """

    sink: Sink

    @abc.abstractmethod
    def exhaust(self, generator: Iterator[pd.DataFrame]):
        raise NotImplementedError