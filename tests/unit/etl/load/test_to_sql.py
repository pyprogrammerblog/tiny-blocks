import pandas as pd

from tiny_blocks.etl.load.to_sql import LoadSQL, KwargsLoadSQL
from tiny_blocks.etl.extract.from_sql_table import (
    ExtractSQLTable,
    KwargsExtractSQLTable,
)


def test_sql_load_into_sink(sqlite_source, sqlite_sink):

    extract_kwargs = KwargsExtractSQLTable(table_name="TEST")
    extract_sql = ExtractSQLTable(source=sqlite_source, kwargs=extract_kwargs)
    generator = extract_sql.get_iter()

    load_kwargs = KwargsLoadSQL(name="TEST")
    load_to_sql = LoadSQL(sink=sqlite_sink, kwargs=load_kwargs)
    load_to_sql.exhaust(generator=generator)

    df = pd.read_sql_table(
        table_name="TEST", con=sqlite_sink.connection_string
    )
    assert df.shape == (3, 3)
