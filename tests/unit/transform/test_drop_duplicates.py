import pytest
import pandas as pd
from tiny_blocks.extract.from_csv import FromCSV
from tiny_blocks.transform.drop_duplicates import DropDuplicates


@pytest.mark.parametrize(
    "subset,expected",
    [({"a"}, (2, 3)), ({"a", "b"}, (1, 3)), ({"a", "b", "c"}, (1, 3))],
)
def test_drop_duplicates(csv_source, subset, expected):

    extract_csv = FromCSV(path=csv_source)
    drop_duplicates = DropDuplicates(subset=subset)

    generator = extract_csv.get_iter()
    generator = drop_duplicates.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == expected


def test_drop_duplicates_no_subset(csv_source):

    extract_csv = FromCSV(path=csv_source)
    drop_duplicates = DropDuplicates()

    generator = extract_csv.get_iter()
    generator = drop_duplicates.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == (3, 3)
