import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable
from tiny_blocks.transform.astype import Astype


def test_astype(sqlite_source, sqlite_sink):

    extract_sql = FromSQLTable(dsn_conn=sqlite_source, table_name="test")
    as_type = Astype(dtype={"e": "float32"})

    source_generator = extract_sql.get_iter()
    generator = as_type.get_iter(source=source_generator)

    # exhaust and assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert str(df.dtypes.d) == "int64"
    assert str(df.dtypes.e) == "float32"
    assert str(df.dtypes.f) == "float64"
