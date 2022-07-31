import pandas as pd
from blocks.etl.extract.from_sql_query import (
    ReadSQLQueryBlock,
    KwargsReadSQLQuery,
)


def test_extract_from_sql(source_sql):

    kwargs = KwargsReadSQLQuery(sql="SELECT * FROM TEST")
    read_sql = ReadSQLQueryBlock(source=source_sql, kwargs=kwargs)
    generator = read_sql.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columnsto_list() == ["a", "b", "c"]
    assert df.shape == (3, 3)
