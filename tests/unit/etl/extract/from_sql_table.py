import pandas as pd
from tiny_blocks.etl.extract.from_sql_table import (
    ExtractSQLTable,
    KwargsExtractSQLTable,
)


def test_extract_from_sql(sql_source):

    kwargs = KwargsExtractSQLTable(table_name="TEST")
    read_sql_table = ExtractSQLTable(source=sql_source, kwargs=kwargs)
    generator = read_sql_table.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columnsto_list() == ["a", "b", "c"]
    assert df.shape == (3, 3)
