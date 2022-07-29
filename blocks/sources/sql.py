from blocks.sources.base import BaseSource
from pydantic import PostgresDsn, Field
from typing import Union
from typing import Literal
import logging

logger = logging.getLogger(__name__)

__all__ = ["SQLSource"]


class SQLSource(BaseSource):
    name: Literal["sql"]
    conn: Union[PostgresDsn] = Field(..., description="Dsn")
    validation_schema: str = None  # TODO
