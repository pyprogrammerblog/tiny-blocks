import pandas as pd

from tiny_blocks.etl.load.to_csv import LoadCSV
from tiny_blocks.etl.extract.from_csv import ExtractCSV, KwargsExtractCSV


def test_csv_load_into_sink(csv_source, csv_sink):

    kwargs = KwargsExtractCSV(table_name="TEST")
    extract_csv = ExtractCSV(source=csv_source, kwargs=kwargs)
    generator = extract_csv.get_iter()

    load_into_csv = LoadCSV(sink=csv_sink)
    load_into_csv.exhaust(generator=generator)

    df = pd.read_csv(index_col=False)
    assert df.shape == (3, 3)
