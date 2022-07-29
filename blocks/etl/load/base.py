import logging
from smart_stream.models.blocks.base import BaseBlock
from smart_stream.models.blocks.dependencies import OneInput
from smart_stream.models.blocks.base import KwargsBase
import pandas as pd
import dask.dataframe as dd
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
    def delayed(self, **blocks: dd.DataFrame):
        raise NotImplementedError

    @abc.abstractmethod
    def dispatch(self, **blocks: pd.DataFrame):
        raise NotImplementedError
