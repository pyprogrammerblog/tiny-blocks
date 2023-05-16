import contextlib
import json
import logging
from typing import Iterator, Literal, List
from kafka import KafkaConsumer
from tiny_blocks.base import Row
from tiny_blocks.extract.base import ExtractBase

logger = logging.getLogger(__name__)


__all__ = ["FromKafka"]


class FromKafka(ExtractBase):
    """FromKafka Block. Defines the read Kafka Operation"""

    name: Literal["from_kafka"] = "from_kafka"
    topic: str
    group_id: str
    bootstrap_servers: List[str]
    consumer_timeout: int = 1000

    @contextlib.contextmanager
    def kafka_consumer(self) -> KafkaConsumer:
        """
        It yields a consumer to a Kafka Consumer.

        Parameters set on the connection are:
            - `topic`.
            - `group_id`.
            - `bootstrap_servers`. List of server strings.
            - `Auto_offset_reset` is set to `True`.
            - `Enable_auto_commit` is set to `True`.
            - `Consumer_timeout_ms` by default to 1 second.
        """
        consumer = KafkaConsumer(
            self.topic,
            group_id=self.group_id,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=self.consumer_timeout,
        )
        try:
            yield consumer
        finally:
            consumer.close()

    def get_iter(self) -> Iterator[Row]:

        with self.kafka_consumer() as consumer:
            for msg_str in consumer:
                yield Row(json.loads(msg_str))
