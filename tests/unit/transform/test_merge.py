import pandas as pd
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.transform.merge import Merge


def test_merge_left(postgres_source, mysql_source):

    postgres = ExtractSQLTable(dsn_conn=postgres_source, table_name="test")
    mysql = ExtractSQLTable(dsn_conn=mysql_source, table_name="test")

    left_gen = mysql.get_iter()
    right_gen = postgres.get_iter()

    merge = Merge(how="left", left_on="c", right_on="c")
    generator = merge.get_iter(left=left_gen, right=right_gen)

    # exhaust and test
    df = pd.concat(generator)
    assert df.shape == (3, 6)
    assert df.columns.to_list() == ["c", "d", "e", "c", "d", "e"]


def test_merge_inner(postgres_source, mysql_source):

    postgres = ExtractSQLTable(dsn_conn=postgres_source, table_name="test")
    mysql = ExtractSQLTable(dsn_conn=mysql_source, table_name="test")

    left_gen = mysql.get_iter()
    right_gen = postgres.get_iter()

    merge = Merge(how="inner", left_on="d", right_on="d")
    generator = merge.get_iter(left=left_gen, right=right_gen)

    # exhaust and test
    df = pd.concat(generator)
    assert df.shape == (3, 6)
    assert df.columns.to_list() == ["c", "d", "e", "c", "d", "e"]
