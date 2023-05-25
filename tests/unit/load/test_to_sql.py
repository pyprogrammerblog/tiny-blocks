import pandas as pd
from tiny_blocks.extract.from_sql import FromSQL
from tiny_blocks.load.to_sql import ToSQL


def test_to_sql(postgres_uri, source_data):

    to_sql = ToSQL(dsn_conn=postgres_uri)
    to_sql.exhaust(source=source_data)

    # assert
    df = pd.read_sql_table(table_name="destination", con=postgres_sink)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["d", "e", "f"]
