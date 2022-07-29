from blocks.sinks.base import BaseSink
from pathlib import Path
from typing import Literal
import logging

logger = logging.getLogger(__name__)


__all__ = ["CSVSink"]


class CSVSink(BaseSink):
    block_name: Literal["csv"] = "csv"
    path: Path
    validation_schema: str = None
