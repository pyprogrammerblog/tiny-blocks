from blocks.sources.base import BaseSource
from typing import Literal
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

__all__ = ["CSVSource"]


class CSVSource(BaseSource):
    name: Literal["csv"]
    path: Path
    validation_schema: str = None  # TODO
