from tiny_blocks.sinks.csv import CSVSink
from tiny_blocks.sinks.sql import SQLSink
from typing import Union


AnySink = Union[CSVSink, SQLSink]
