from tiny_blocks.sources.base import BaseSource
from contextlib import contextmanager
from sqlalchemy import create_engine
from pydantic import Field
from typing import Literal
import logging

logger = logging.getLogger(__name__)

__all__ = ["SQLSource"]


class SQLSource(BaseSource):
    name: Literal["sql_source"] = "sql_source"
    connection_string: str = Field(description="Connection string")

    @contextmanager
    def connect(self):
        engine = create_engine(self.connection_string)
        conn = engine.connect()
        conn.execution_options(stream_results=True, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()
            engine.dispose()
