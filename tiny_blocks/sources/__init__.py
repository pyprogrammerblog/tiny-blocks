from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.sources.sql import SQLiteSource
from typing import Union


Source = Union[CSVSource, SQLiteSource]
SQLSource = Union[SQLiteSource]
