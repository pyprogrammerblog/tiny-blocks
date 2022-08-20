import logging
from typing import Iterator
import pandas as pd
from tiny_blocks.base import BaseBlock
from tiny_blocks.load.base import KwargsBase
from tiny_blocks.base import MixinExtract


__all__ = ["ExtractBase", "KwargsExtractBase"]


logger = logging.getLogger(__name__)


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBase(BaseBlock, MixinExtract):
    """
    Extract Base Block.

    Each extraction Block implement the `get_iter` method.
    This method return an Iterator of chunked DataFrames
    """

    def get_iter(self) -> Iterator[pd.DataFrame]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        extraction block
        """
        raise NotImplementedError
