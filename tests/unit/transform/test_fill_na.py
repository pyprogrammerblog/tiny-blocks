import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable
from tiny_blocks.transform.fillna import Fillna


def test_fillna(sqlite_source, sqlite_sink):

    extract_sql = FromSQLTable(dsn_conn=sqlite_source, table_name="test")
    fill_na = Fillna(value="Hola Mundo")

    source = extract_sql.get_iter()
    generator = fill_na.get_iter(source=source)

    # assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["d", "e", "f"]
    assert df.iloc[2, 2] == "Hola Mundo"
