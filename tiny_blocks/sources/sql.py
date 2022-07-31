from tiny_blocks.sources.base import BaseSource
from pydantic import Field
from typing import Literal
import logging

logger = logging.getLogger(__name__)

__all__ = ["SQLiteSource"]


class SQLiteSource(BaseSource):
    name: Literal["sqlite3_source"] = "sqlite3_source"
    conn_string: str = Field(description="Connection string")
