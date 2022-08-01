import pandas as pd

from tiny_blocks.etl.transform.dropna import DropNa
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable


def test_sql_load_into_sqlite(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(source=sqlite_source, table_name="test")
    drop_na = DropNa()

    generator = extract_sql.get_iter()
    generator = drop_na.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == (2, 3)
