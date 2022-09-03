import pandas as pd
from tiny_blocks.extract.from_sql_query import FromSQLQuery


def test_extract_from_sqlite(sqlite_source):

    read_sql = FromSQLQuery(dsn_conn=sqlite_source, sql="select * from test")
    generator = read_sql.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["d", "e", "f"]
    assert df.shape == (3, 3)


def test_extract_from_sql_postgres(postgres_source):

    read_sql = FromSQLQuery(dsn_conn=postgres_source, sql="select * from test")
    generator = read_sql.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["d", "e", "f"]
    assert df.shape == (3, 3)


def test_extract_from_sql_mysql(mysql_source):

    read_sql = FromSQLQuery(dsn_conn=mysql_source, sql="select * from test")
    generator = read_sql.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["c", "d", "e"]
    assert df.shape == (3, 3)
