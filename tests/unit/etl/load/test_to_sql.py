import pandas as pd

from tiny_blocks.etl.load.to_sql import LoadSQL
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable


def test_sql_load_into_sqlite(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(source=sqlite_source, table_name="TEST")
    load_to_sql = LoadSQL(sink=sqlite_sink, table_name="DESTINATION")

    load_to_sql.exhaust(generator=extract_sql.get_iter())

    # assert
    df = pd.read_sql_table(
        table_name="DESTINATION", con=sqlite_sink.connection_string
    )
    assert df.shape == (3, 3)


def test_sql_load_into_postgres(postgres_source, postgres_sink):

    extract_sql = ExtractSQLTable(source=postgres_source, table_name="TEST")
    load_to_sql = LoadSQL(sink=postgres_sink, table_name="DESTINATION")

    load_to_sql.exhaust(generator=extract_sql.get_iter())

    # assert
    df = pd.read_sql_table(
        table_name="DESTINATION", con=postgres_sink.connection_string
    )
    assert df.shape == (3, 3)


def test_sql_load_into_mysql(mysql_source, mysql_sink):

    extract_sql = ExtractSQLTable(source=mysql_source, table_name="TEST")
    load_to_sql = LoadSQL(sink=mysql_sink, table_name="DESTINATION")

    load_to_sql.exhaust(generator=extract_sql.get_iter())

    # assert
    df = pd.read_sql_table(
        table_name="DESTINATION", con=mysql_sink.connection_string
    )
    assert df.shape == (3, 3)
