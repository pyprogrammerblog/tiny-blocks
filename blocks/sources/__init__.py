from blocks.sources.csv import CSVSource
from blocks.sources.json import JSONSource
from blocks.sources.sql import SQLSource
from typing import Union


AnySource = Union[CSVSource, JSONSource, SQLSource]
