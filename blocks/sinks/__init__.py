from blocks.sinks.csv import CSVSink
from blocks.sinks.json import JSONSink
from blocks.sinks.sql import SQLSink
from typing import Union


AnySink = Union[CSVSink, JSONSink, SQLSink]
