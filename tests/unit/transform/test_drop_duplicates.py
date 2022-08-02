import pytest
import pandas as pd
from tiny_blocks.extract.from_csv import ExtractCSV
from tiny_blocks.transform.drop_duplicates import (
    DropDuplicates,
    KwargsDropDuplicates,
)


@pytest.mark.parametrize(
    "subset,expected",
    [({"a"}, (2, 3)), ({"a", "b"}, (1, 3)), ({"a", "b", "c"}, (1, 3))],
)
def test_drop_duplicates(csv_source, subset, expected):

    extract_csv = ExtractCSV(path=csv_source)
    kwargs = KwargsDropDuplicates(subset=subset)
    drop_duplicates = DropDuplicates(kwargs=kwargs)

    generator = extract_csv.get_iter()
    generator = drop_duplicates.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == expected


def test_drop_duplicates_no_subset(csv_source):

    extract_csv = ExtractCSV(path=csv_source)
    drop_duplicates = DropDuplicates()

    generator = extract_csv.get_iter()
    generator = drop_duplicates.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == (3, 3)
