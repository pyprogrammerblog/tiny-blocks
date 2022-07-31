from tiny_blocks.sources.base import BaseSource
from pydantic import Field
from typing import Literal
import logging

logger = logging.getLogger(__name__)

__all__ = ["SQLSource"]


class SQLSource(BaseSource):
    name: Literal["sql_source"] = "sql_source"
    conn: str = Field(description="Connection string")
    validation_schema: str = None  # TODO
