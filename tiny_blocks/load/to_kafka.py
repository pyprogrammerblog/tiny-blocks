import logging
import contextlib
from typing import Iterator, Literal, List

import pandas as pd
from kafka import KafkaProducer
from tiny_blocks.load.base import KwargsLoadBase, LoadBase

__all__ = ["ToKafka", "KwargsToKafka"]


logger = logging.getLogger(__name__)


class KwargsToKafka(KwargsLoadBase):
    """"""

    consumer_timeout: int = 1000


class ToKafka(LoadBase):
    """Write CSV Block. Defines the load to CSV Operation"""

    name: Literal["to_csv"] = "to_kafka"
    kwargs: KwargsToKafka = KwargsToKafka()
    topic: str
    group_id: str
    bootstrap_servers: List[str]

    @contextlib.contextmanager
    def kafka_producer(self) -> KafkaProducer:
        producer = KafkaProducer(
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=self.kwargs.consumer_timeout,
        )
        try:
            yield producer
        finally:
            producer.close()

    def exhaust(self, generator: Iterator[pd.DataFrame]):

        with self.kafka_producer() as producer:
            for chunk in generator:
                json_msg = chunk.to_json()
                producer.send(self.topic, json_msg)
