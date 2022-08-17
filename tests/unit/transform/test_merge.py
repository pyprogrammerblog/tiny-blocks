import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable
from tiny_blocks.transform.merge import Merge


def test_merge_left(postgres_source, mysql_source):

    postgres = FromSQLTable(dsn_conn=postgres_source, table_name="test")
    mysql = FromSQLTable(dsn_conn=mysql_source, table_name="test")

    left_gen = mysql.get_iter()
    right_gen = postgres.get_iter()

    merge = Merge(how="left", left_on="c", right_on="d")
    generator = merge.get_iter(source=[left_gen, right_gen])

    # exhaust and test
    df = pd.concat(generator)
    assert df.shape == (3, 6)
    assert df.columns.to_list() == ["c", "d", "e", "d", "e", "f"]


def test_merge_inner(postgres_source, mysql_source):

    postgres = FromSQLTable(dsn_conn=postgres_source, table_name="test")
    mysql = FromSQLTable(dsn_conn=mysql_source, table_name="test")

    left_gen = mysql.get_iter()
    right_gen = postgres.get_iter()

    merge = Merge(how="inner", left_on="c", right_on="d")
    generator = merge.get_iter(source=[left_gen, right_gen])

    # exhaust and test
    df = pd.concat(generator)
    assert df.shape == (3, 6)
    assert df.columns.to_list() == ["c", "d", "e", "d", "e", "f"]
