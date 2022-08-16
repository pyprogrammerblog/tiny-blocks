import logging
import abc
from typing import Iterator
import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase


__all__ = ["TransformBase", "KwargsTransformBase"]


logger = logging.getLogger(__name__)


class KwargsTransformBase(KwargsBase):
    pass


class TransformBase(BaseBlock, abc.ABC):
    """
    Transform Base Block

    Each transformation Block implements the `get_iter` method.
    This method get one or multiple iterators and return
    an Iterator of chunked DataFrames.
    """

    @abc.abstractmethod
    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        transformation block
        """
        raise NotImplementedError


class TransformTwoInputsBase(BaseBlock, abc.ABC):
    """
    Transform Two Input Base Block

    Two sources are transform by the same block

    Each transformation Block implements the `get_iter` method.
    This method get one or multiple iterators and return
    an Iterator of chunked DataFrames.
    """

    @abc.abstractmethod
    def get_iter(
        self, left: Iterator[pd.DataFrame], right: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        transformation block
        """
        raise NotImplementedError
