import pandas as pd
from tiny_blocks.etl.extract.from_sql_query import (
    ExtractSQLQuery,
    KwargsExtractSQLQuery,
)


def test_extract_from_sql(sql_source):

    kwargs = KwargsExtractSQLQuery(sql="SELECT * FROM TEST")
    read_sql = ExtractSQLQuery(source=sql_source, kwargs=kwargs)
    generator = read_sql.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columnsto_list() == ["a", "b", "c"]
    assert df.shape == (3, 3)
