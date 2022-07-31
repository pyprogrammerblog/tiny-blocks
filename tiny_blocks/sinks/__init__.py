from tiny_blocks.sinks.csv import CSVSink
from tiny_blocks.sinks.sql import SQLiteSink
from typing import Union


AnySink = Union[CSVSink, SQLiteSink]
AnySQLSink = Union[SQLiteSink]
