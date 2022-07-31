import pandas as pd
from blocks.etl.extract.read_csv import ReadCSVBlock


def test_extract_from_csv(source_csv):

    read_csv = ReadCSVBlock(source=source_csv)
    generator = read_csv.get_iter()

    # exhaust the generator and validate
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["a", "b", "c"]
