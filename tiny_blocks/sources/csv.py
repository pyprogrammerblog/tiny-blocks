from tiny_blocks.sources.base import BaseSource
from typing import Literal
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

__all__ = ["CSVSource"]


class CSVSource(BaseSource):
    name: Literal["csv_source"] = "csv_source"
    path: Path
