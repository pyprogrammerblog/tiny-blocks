from contextlib import contextmanager
from tiny_blocks.sinks.base import BaseSink
from pydantic import Field
from typing import Literal
import logging
import sqlite3

logger = logging.getLogger(__name__)


__all__ = ["SQLiteSink"]


class SQLiteSink(BaseSink):

    block_name: Literal["sql"] = "sqlite_sink"
    connection_string: str = Field(..., description="Connection string")

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.connection_string)
        try:
            yield conn
        finally:
            conn.close()
