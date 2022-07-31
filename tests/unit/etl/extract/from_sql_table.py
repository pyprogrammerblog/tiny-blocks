import pandas as pd
from blocks.etl.extract.from_sql_table import (
    ReadSQLTableBlock,
    KwargsReadSQLTable,
)


def test_extract_from_sql(source_sql):

    kwargs = KwargsReadSQLTable(table_name="TEST")
    read_sql_table = ReadSQLTableBlock(source=source_sql, kwargs=kwargs)
    generator = read_sql_table.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columnsto_list() == ["a", "b", "c"]
    assert df.shape == (3, 3)
