from tiny_blocks.extract.from_csv import FromCSV


def test_extract_from_sql(csv_source):

    from_sql = FromCSV(path=csv_source.name)
    generator = from_sql.get_iter()

    # exhaust the generator
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert data[0].columns() == ["name", "age"]
    assert data[0].values() == ["Mateo", 30]
