import logging
from typing import Iterator
from blocks.etl.base import BaseBlock, KwargsBase
import pandas as pd
import abc


__all__ = ["TransformBlock", "KwargsTransformBlock"]


logger = logging.getLogger(__name__)


class KwargsTransformBlock(KwargsBase):
    pass


class TransformBlock(BaseBlock):
    """
    Extract Base Block
    """
    @abc.abstractmethod
    def get_iter(self, **blocks: Iterator[pd.DataFrame]):
        raise NotImplementedError

    @abc.abstractmethod
    def process(self, **blocks: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
