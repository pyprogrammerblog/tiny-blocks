import pandas as pd
from tiny_blocks.extract.from_csv import ExtractCSV
from tiny_blocks.load.to_csv import LoadCSV


def test_csv_load_into_sink(csv_source, csv_sink):

    extract_csv = ExtractCSV(path=csv_source)
    load_into_csv = LoadCSV(path=csv_sink)

    generator = extract_csv.get_iter()
    load_into_csv.exhaust(generator=generator)

    df = pd.read_csv(csv_source, sep="|")
    assert df.shape == (4, 3)
