import abc
import logging
import functools
from typing import Iterator
from tiny_blocks.etl.base import BaseBlock, KwargsBase
from tiny_blocks.sources import Source

import pandas as pd

__all__ = ["ExtractBase", "KwargsExtractBase", "check_types"]


logger = logging.getLogger(__name__)


def check_types(extract_func):
    @functools.wraps(extract_func)
    def decorator(block: "ExtractBase"):
        data = extract_func(block)
        # validate after extraction (if validation_schema exists)
        if validation_schema := block.source.validation_schema:
            validation_schema.validate(data)
        return data

    return decorator


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBase(BaseBlock):
    """
    Extract Base Block
    """

    source: Source

    @abc.abstractmethod
    @check_types
    def get_iter(self) -> Iterator[pd.DataFrame]:
        raise NotImplementedError