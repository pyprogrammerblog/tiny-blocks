import pandas as pd

from tiny_blocks.etl.transform.astype import Astype
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable


def test_sql_load_into_sqlite(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(source=sqlite_source, table_name="test")
    as_type = Astype(dtype={"e": "float32"})

    generator = extract_sql.get_iter()
    generator = as_type.get_iter(generator=generator)

    # exhaust and assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert str(df.dtypes.c) == "int64"
    assert str(df.dtypes.d) == "int64"
    assert str(df.dtypes.e) == "float32"
