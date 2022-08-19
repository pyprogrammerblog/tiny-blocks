import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable
from tiny_blocks.transform.apply import Apply


def test_apply(sqlite_source):

    extract_sql = FromSQLTable(dsn_conn=sqlite_source, table_name="test")
    enrich = Apply(
        func=lambda x: x + 1,
        apply_to_column="d",
        set_to_column="new",
    )

    source = extract_sql.get_iter()
    generator = enrich.get_iter(source=source)

    # assert
    df = pd.concat(generator)
    assert df.shape == (3, 4)
    assert df.columns.to_list() == ["d", "e", "f", "new"]
    assert df.new.to_list() == [2, 3, 4]
