import logging
from typing import Iterator, Literal

import pandas as pd
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["ToKafka", "KwargsToKafka"]


logger = logging.getLogger(__name__)


class KwargsToKafka(KwargsLoadBase):
    """"""

    pass


class ToKafka(LoadBase):
    """Write CSV Block. Defines the load to CSV Operation"""

    name: Literal["to_csv"] = "to_kafka"
    kwargs: KwargsToKafka = KwargsToKafka()

    def exhaust(self, generator: Iterator[pd.DataFrame]):
        pass
