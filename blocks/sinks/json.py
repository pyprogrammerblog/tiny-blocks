from smart_stream.models.sinks.base import BaseSink
from typing import Literal
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

__all__ = ["JSONSink"]


class JSONSink(BaseSink):
    block_name: Literal["json"]
    path: str
    validation_schema: str = None
