import contextlib
import json
import logging
from kafka import KafkaConsumer
from typing import Iterator, Literal, List, Type
from tiny_blocks.extract.base import ExtractBase
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


__all__ = ["FromKafka"]


class FromKafka(ExtractBase):
    """FromKafka Block. Defines the read Kafka Operation"""

    name: Literal["from_kafka"] = Field(default="from_kafka")
    row_model: Type[BaseModel] = Field(..., description="Row model")
    topic: str = Field(..., description="Kafka topic")
    group_id: str = Field(..., description="Group Id")
    bootstrap_servers: List[str] = Field(..., description="Bootstrap servers")
    consumer_timeout: int = Field(default=1000, description="Consumer Timeout")

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

    def get_iter(self) -> Iterator[BaseModel]:

        with self.kafka_consumer() as consumer:
            for msg_str in consumer:
                yield self.row_model(**json.loads(msg_str))
