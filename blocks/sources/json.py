from blocks.sources.base import BaseSource
from typing import Literal
import logging

logger = logging.getLogger(__name__)

__all__ = ["JSONSource"]


class JSONSource(BaseSource):
    name: Literal["json"]
    path: str
    validation_schema: str = None  # TODO

    # @validator("path")
    # def validate_blocks(cls, v):
    #     try:
    #         Path(v)
