from typing import Union
from tiny_blocks.load.to_csv import ToCSV, KwargsToCSV
from tiny_blocks.load.to_sql import ToSQL, KwargsToSQL
from tiny_blocks.load.to_kafka import ToKafka, KwargsToKafka


LoadBlocks = Union[ToCSV | ToSQL | ToKafka]

__all__ = [
    "ToCSV",
    "KwargsToCSV",
    "ToSQL",
    "KwargsToSQL",
    "ToKafka",
    "KwargsToKafka",
]
