from tiny_blocks.sinks.base import BaseSink
from pydantic import Field
from typing import Literal
import logging

logger = logging.getLogger(__name__)


__all__ = ["SQLSink"]


class SQLSink(BaseSink):
    block_name: Literal["sql"] = "sql_sink"
    conn: str = Field(..., description="Connection string")
    validation_schema: str = None
