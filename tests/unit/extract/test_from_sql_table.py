import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable


def test_extract_from_sqlite(sqlite_source):

    read_sql_table = FromSQLTable(dsn_conn=sqlite_source, table_name="TEST")
    generator = read_sql_table.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["d", "e", "f"]
    assert df.shape == (3, 3)


def test_extract_from_postgres(postgres_source):

    read_sql_table = FromSQLTable(dsn_conn=postgres_source, table_name="test")
    generator = read_sql_table.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["d", "e", "f"]
    assert df.shape == (3, 3)


def test_extract_from_mysql(mysql_source):

    read_sql_table = FromSQLTable(dsn_conn=mysql_source, table_name="test")
    generator = read_sql_table.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["c", "d", "e"]
    assert df.shape == (3, 3)


# def test_extract_from_oracle(oracle_source):
#
#     read_sql_table = FromSQLTable(dsn_conn=oracle_source, table_name="test")
#     generator = read_sql_table.get_iter()
#
#     # exhaust the generator and validate
#     df = pd.concat(generator)
#     assert df.columns.to_list() == ["c", "d", "e"]
#     assert df.shape == (3, 3)
