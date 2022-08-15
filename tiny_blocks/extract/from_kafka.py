import logging
from typing import Iterator, Literal

import pandas as pd
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["FromKafka", "KwargsFromKafka"]


class KwargsFromKafka(KwargsExtractBase):
    """"""

    pass


class FromKafka(ExtractBase):
    """FromKafka Block. Defines the read Kafka Operation"""

    name: Literal["read_csv"] = "from_kafka"
    kwargs: KwargsFromKafka = KwargsFromKafka()

    def get_iter(self) -> Iterator[pd.DataFrame]:
        pass
