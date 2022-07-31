import pandas as pd

from tiny_blocks.etl.load.to_sql import LoadSQL
from tiny_blocks.etl.extract.from_sql_table import (
    ExtractSQLTable,
    KwargsExtractSQLTable,
)


def test_sql_load_into_sink(sql_source, sql_sink):

    kwargs = KwargsExtractSQLTable(table_name="TEST")
    extract_sql = ExtractSQLTable(source=sql_source, kwargs=kwargs)
    generator = extract_sql.get_iter()

    load_into_sql = LoadSQL(sink=sql_sink)
    load_into_sql.exhaust(generator=generator)

    df = pd.read_sql_table(table_name="TEST", con=sql_sink.conn)
    assert df.shape == (3, 3)
