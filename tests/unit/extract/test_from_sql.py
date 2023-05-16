from tiny_blocks.extract.from_sql import FromSQL


def test_extract_from_sql(postgres_source):

    from_sql = FromSQL(dsn_conn=postgres_source, sql="select * from Users")
    generator = from_sql.get_iter()

    # exhaust the generator
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert data[0].columns() == ["name", "active", "created"]
    assert data[0].values() == ["Mateo", 1, "2022-01-10"]
