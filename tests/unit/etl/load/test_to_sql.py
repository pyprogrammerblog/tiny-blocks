import pandas as pd

from tiny_blocks.etl.load.to_sql import LoadSQL
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable


def test_sql_load_into_sink(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(source=sqlite_source, table_name="TEST")
    generator = extract_sql.get_iter()

    load_to_sql = LoadSQL(sink=sqlite_sink, table_name="DESTINATION")
    load_to_sql.exhaust(generator=generator)

    df = pd.read_sql_table(
        table_name="DESTINATION", con=sqlite_sink.connection_string
    )
    assert df.shape == (3, 3)
