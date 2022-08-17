import logging
from typing import Generator

import pandas as pd
from tiny_blocks.base import BaseBlock, KwargsBase

__all__ = ["ExtractBase", "KwargsExtractBase"]


logger = logging.getLogger(__name__)


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBase(BaseBlock):
    """
    Extract Base Block.

    Each extraction Block implement the `get_iter` method.
    This method return an Iterator of chunked DataFrames
    """

    def get_iter(self) -> Generator[pd.DataFrame, None, None]:
        """
        Return an iterator of chunked dataframes

        The `chunksize` is defined as kwargs in each
        extraction block
        """
        raise NotImplementedError
