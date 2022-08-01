import pandas as pd

from tiny_blocks.etl.transform.rename import Rename
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable


def test_sql_load_into_sqlite(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(source=sqlite_source, table_name="test")
    as_type = Rename(columns={"c": "Hola", "d": "a", "e": "todos"})

    generator = extract_sql.get_iter()
    generator = as_type.get_iter(generator=generator)

    # exhaust and assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["Hola", "a", "todos"]
