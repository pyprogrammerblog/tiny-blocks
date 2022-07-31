from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.sources.json import JSONSource
from tiny_blocks.sources.sql import SQLSource
from typing import Union


AnySource = Union[CSVSource, JSONSource, SQLSource]
