from tiny_blocks.transform.dropna import DropNone


def test_drop_na(sqlite_source, sqlite_sink):

    drop_na = DropNone()
    generator = drop_na.get_iter(source=source)

    # exhaust the generator and assert data
    data = list(generator)
    assert len(data) == 3
