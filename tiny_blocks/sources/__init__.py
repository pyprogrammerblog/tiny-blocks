from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.sources.sql import SQLSource
from typing import Union

Source = Union[CSVSource, SQLSource]
