from smart_stream.models.sinks.csv import CSVSink
from smart_stream.models.sinks.json import JSONSink
from smart_stream.models.sinks.sql import SQLSink
from typing import Union


Sources = Union[CSVSink, JSONSink, SQLSink]
