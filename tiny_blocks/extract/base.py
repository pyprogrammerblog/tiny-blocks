import logging
from tiny_blocks.base import ExtractBase
from tiny_blocks.load.base import KwargsBase


__all__ = ["ExtractBase", "KwargsExtractBase"]


logger = logging.getLogger(__name__)


class KwargsExtractBase(KwargsBase):
    """
    Kwargs Extract Block
    """

    pass
