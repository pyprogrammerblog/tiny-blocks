from tiny_blocks.sinks.base import BaseSink
from pydantic import Field
from typing import Literal
import logging

logger = logging.getLogger(__name__)


__all__ = ["SQLiteSink"]


class SQLiteSink(BaseSink):

    block_name: Literal["sql"] = "sqlite_sink"
    conn_string: str = Field(..., description="Connection string")
