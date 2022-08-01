import pandas as pd
from tiny_blocks.etl.extract.from_sql_query import ExtractSQLQuery


def test_extract_from_sql(sqlite_source):

    read_sql = ExtractSQLQuery(source=sqlite_source, sql="SELECT * FROM TEST")
    generator = read_sql.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.columns.to_list() == ["c", "d", "e"]
    assert df.shape == (3, 3)
