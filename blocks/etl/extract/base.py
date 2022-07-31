import abc
import logging
import functools
from typing import Iterator
from blocks.etl.base import BaseBlock, KwargsBase
from blocks.sources import AnySource

import pandas as pd

__all__ = ["ExtractBlock", "KwargsExtractBlock", "check_types"]


logger = logging.getLogger(__name__)


def check_types(extract_func):
    @functools.wraps(extract_func)
    def decorator(block: "ExtractBlock"):
        data = extract_func(block)
        # validate after extraction (if validation_schema exists)
        if validation_schema := block.source.validation_schema:
            validation_schema.validate(data)
        return data

    return decorator


class KwargsExtractBlock(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass


class ExtractBlock(BaseBlock):
    """
    Extract Base Block
    """

    source: AnySource

    @abc.abstractmethod
    @check_types
    def get_iter(self) -> Iterator[pd.DataFrame]:
        raise NotImplementedError
