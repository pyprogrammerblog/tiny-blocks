import logging
from blocks.etl.base import BaseBlock
from blocks.etl.base import KwargsBase
import pandas as pd
import abc


__all__ = ["LoadBlock", "KwargsLoadBlock"]


logger = logging.getLogger(__name__)


class KwargsLoadBlock(KwargsBase):
    pass


class LoadBlock(BaseBlock):
    """
    Load Base Block
    """

    @abc.abstractmethod
    def process(self, **blocks: pd.DataFrame):
        raise NotImplementedError
