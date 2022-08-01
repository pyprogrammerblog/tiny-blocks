import logging
import contextlib
from sqlalchemy import create_engine
from tiny_blocks.sinks.base import BaseSink
from pydantic import Field
from typing import Literal


logger = logging.getLogger(__name__)


__all__ = ["SQLSink"]


class SQLSink(BaseSink):

    block_name: Literal["sql"] = "sqlite_sink"
    connection_string: str = Field(..., description="Connection string")

    @contextlib.contextmanager
    def connect(self):
        engine = create_engine(self.connection_string, echo="debug")
        conn = engine.connect()
        conn.execution_options(stream_results=True, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
            engine.dispose()
