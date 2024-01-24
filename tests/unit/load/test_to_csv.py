from pathlib import Path
from tiny_blocks.extract.from_csv import FromCSV
from tiny_blocks.load.to_csv import ToCSV


def test_csv_load_into_sink(csv_source, csv_sink):

    extract_csv = FromCSV(path=csv_source)
    load_into_csv = ToCSV(path=csv_sink)

    generator = extract_csv.get_iter()
    load_into_csv.exhaust(source=generator)

    # validations
    assert Path(csv_sink).exists()
