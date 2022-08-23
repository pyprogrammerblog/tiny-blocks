from typing import Union
from tiny_blocks.extract.from_csv import (
    FromCSV,
    KwargsFromCSV,
)
from tiny_blocks.extract.from_sql_table import (
    FromSQLTable,
    KwargsFromSQLTable,
)
from tiny_blocks.extract.from_sql_query import (
    KwargsFromSQLQuery,
    FromSQLQuery,
)
from tiny_blocks.extract.from_kafka import (
    KwargsFromKafka,
    FromKafka,
)

ExtractBlocks = Union[FromCSV | FromSQLTable | FromSQLQuery | FromKafka]


__all__ = [
    "FromCSV",
    "KwargsFromCSV",
    "FromSQLTable",
    "KwargsFromSQLTable",
    "FromSQLQuery",
    "KwargsFromSQLQuery",
    "FromKafka",
    "KwargsFromKafka",
]
