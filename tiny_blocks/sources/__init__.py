from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.sources.sql import SQLiteSource
from typing import Union


AnySource = Union[CSVSource, SQLiteSource]
AnySQLSource = Union[SQLiteSource]
