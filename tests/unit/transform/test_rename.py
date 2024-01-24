from tiny_blocks.transform.rename import Rename


def test_rename(source_data):

    rename = Rename(columns={"name": "nombre", "age": "edad"})
    generator = rename.get_iter(source=source_data)

    # exhaust and assert
    data = list(generator)
    assert len(data) == 4
    assert data[0].columns() == ["nombre", "edad"]
