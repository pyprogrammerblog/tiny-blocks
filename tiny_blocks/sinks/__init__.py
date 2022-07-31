from tiny_blocks.sinks.csv import CSVSink
from tiny_blocks.sinks.sql import SQLiteSink
from typing import Union


Sink = Union[CSVSink, SQLiteSink]
SQLSink = Union[SQLiteSink]
