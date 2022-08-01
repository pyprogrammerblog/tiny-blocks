import pandas as pd
import pytest
from tiny_blocks.etl.transform.drop_duplicates import (
    DropDuplicates,
    KwargsDropDuplicates,
)
from tiny_blocks.etl.extract.from_csv import ExtractCSV


@pytest.mark.parametrize(
    "subset,expected",
    [({"a"}, (3, 3)), ({"a", "b"}, (3, 3)), ({"a", "b", "c"}, (3, 3))],
)
def test_drop_duplicates(csv_source, subset, expected):

    extract_csv = ExtractCSV(source=csv_source)
    kwargs = KwargsDropDuplicates(subset=subset)
    drop_duplicates = DropDuplicates(kwargs=kwargs)

    generator = extract_csv.get_iter()
    generator = drop_duplicates.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == expected
