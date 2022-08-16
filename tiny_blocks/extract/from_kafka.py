import contextlib
import logging
from typing import Iterator, Literal, List

import pandas as pd
from kafka import KafkaConsumer
from tiny_blocks.extract.base import ExtractBase, KwargsExtractBase

logger = logging.getLogger(__name__)


__all__ = ["FromKafka", "KwargsFromKafka"]


class KwargsFromKafka(KwargsExtractBase):
    """"""

    consumer_timeout: int = 1000


class FromKafka(ExtractBase):
    """FromKafka Block. Defines the read Kafka Operation"""

    name: Literal["from_kafka"] = "from_kafka"
    kwargs: KwargsFromKafka = KwargsFromKafka()
    topic: str
    group_id: str
    bootstrap_servers: List[str]

    @contextlib.contextmanager
    def kafka_consumer(self) -> KafkaConsumer:
        """
        Yields a consumer to a Kafka topic.

        Parameters set on the connection are:
            - `topic`.
            - `group_id`.
            - `bootstrap_servers`. List of server strings.
            - `auto_offset_reset` is set to `True`.
            - `enable_auto_commit` is set to `True`.
            - `consumer_timeout_ms` by default to 1 second.
        """
        consumer = KafkaConsumer(
            self.topic,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=self.kwargs.consumer_timeout,
        )
        try:
            yield consumer
        finally:
            consumer.close()

    def get_iter(self) -> Iterator[pd.DataFrame]:

        with self.kafka_consumer() as consumer:
            for msg_str in consumer:
                chunk = pd.json_normalize(msg_str)
                yield chunk
