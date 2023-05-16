from tiny_blocks.transform.drop_duplicates import DropDuplicates


def test_drop_duplicates(source_data):

    drop_duplicates = DropDuplicates()
    generator = drop_duplicates.get_iter(source=source_data)

    # exhaust the generator and assert data
    data = list(generator)
    assert len(data) == 3


def test_drop_duplicates_no_subset(source_data):

    drop_duplicates = DropDuplicates()
    generator = drop_duplicates.get_iter(source=source_data)

    # exhaust the generator and assert data
    data = list(generator)
    assert len(data) == 3
