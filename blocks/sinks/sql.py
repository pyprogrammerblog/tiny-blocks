from smart_stream.models.sinks.base import BaseSink
from pydantic import PostgresDsn, Field
from typing import Union, Literal
import logging

logger = logging.getLogger(__name__)


__all__ = ["SQLSink"]


class SQLSink(BaseSink):
    block_name: Literal["sql"]
    conn: Union[PostgresDsn] = Field(..., description="Dsn")
    validation_schema: str = None
