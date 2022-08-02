import pandas as pd
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.load.to_sql import LoadSQL


def test_sql_load_into_sqlite(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(dsn_conn=sqlite_source, table_name="test")
    load_to_sql = LoadSQL(dsn_conn=sqlite_sink, table_name="destination")

    generator = extract_sql.get_iter()
    load_to_sql.exhaust(generator=generator)

    # assert
    df = pd.read_sql_table(table_name="destination", con=sqlite_sink)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["c", "d", "e"]


def test_sql_load_into_postgres(postgres_source, postgres_sink):

    extract_sql = ExtractSQLTable(dsn_conn=postgres_source, table_name="test")
    load_to_sql = LoadSQL(dsn_conn=postgres_sink, table_name="destination")

    generator = extract_sql.get_iter()
    load_to_sql.exhaust(generator=generator)

    # assert
    df = pd.read_sql_table(table_name="destination", con=postgres_sink)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["c", "d", "e"]


def test_sql_load_into_mysql(mysql_source, mysql_sink):

    extract_sql = ExtractSQLTable(dsn_conn=mysql_source, table_name="test")
    load_to_sql = LoadSQL(dsn_conn=mysql_sink, table_name="destination")

    generator = extract_sql.get_iter()
    load_to_sql.exhaust(generator=generator)

    # assert
    df = pd.read_sql_table(table_name="destination", con=mysql_sink)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["c", "d", "e"]
