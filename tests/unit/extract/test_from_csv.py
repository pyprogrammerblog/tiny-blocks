import pandas as pd
from tiny_blocks.extract.from_csv import ExtractCSV


def test_extract_from_csv(csv_source):

    read_csv = ExtractCSV(path=csv_source)
    generator = read_csv.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.shape == (4, 3)
    assert df.columns.to_list() == ["a", "b", "c"]
