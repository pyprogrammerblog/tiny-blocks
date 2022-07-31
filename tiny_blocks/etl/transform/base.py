import logging
from typing import Iterator
from tiny_blocks.etl.base import BaseBlock, KwargsBase
import pandas as pd
import abc


__all__ = ["TransformBase", "KwargsTransformBase"]


logger = logging.getLogger(__name__)


class KwargsTransformBase(KwargsBase):
    pass


class TransformBase(BaseBlock):
    """
    Extract Base Block
    """

    @abc.abstractmethod
    def get_iter(
        self, *generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        raise NotImplementedError
