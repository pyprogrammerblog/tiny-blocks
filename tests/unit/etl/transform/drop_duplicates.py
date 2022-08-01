import pandas as pd

from tiny_blocks.etl.transform.drop_duplicates import (
    DropDuplicates,
    KwargsDropDuplicates,
)
from tiny_blocks.etl.extract.from_csv import ExtractCSV


def test_drop_duplicates(csv_source):

    kwargs = KwargsDropDuplicates(subset={"a", "b"})
    extract_csv = ExtractCSV(source=csv_source, kwargs=kwargs)
    drop_duplicates = DropDuplicates()

    generator = extract_csv.get_iter()
    generator = drop_duplicates.get_iter(generator=generator)

    # exhaust the generator and assert data
    df = pd.concat(generator)
    assert df.shape == (3, 3)
