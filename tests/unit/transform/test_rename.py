import pandas as pd
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.transform.rename import Rename


def test_rename(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(dsn_conn=sqlite_source, table_name="test")
    as_type = Rename(columns={"d": "Hola", "e": "a", "f": "todos"})

    generator = extract_sql.get_iter()
    generator = as_type.get_iter(generator=generator)

    # exhaust and assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["Hola", "a", "todos"]
