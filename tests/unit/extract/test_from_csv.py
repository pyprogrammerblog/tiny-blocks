from tiny_blocks.extract.from_csv import FromCSV
from pydantic import ValidationError
from tests.conftest import Hero
from datetime import datetime
from pathlib import Path


def test_extract_from_csv(csv_source):

    from_csv = FromCSV(path=csv_source.name, row_model=Hero)
    generator = from_csv.get_iter()

    # exhaust the generator
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert list(data[0].dict().keys()) == [
        "name",
        "secret_name",
        "age",
        "created",
    ]


def test_extract_from_csv_extrict_row_validation(csv_source):

    from_csv = FromCSV(path=csv_source.name, row_model=Hero)
    generator = from_csv.get_iter()

    # exhaust the generator to get validation errors
    try:
        list(generator)
    except ValidationError as error:
        assert len(error.errors()) == 2
        assert "2 validation errors for MyRowModel" in str(error)


def test_extract_from_csv_path_exists(csv_source):

    try:
        FromCSV(path=Path("path/to/file.csv"), row_model=Hero)
    except ValueError as err:
        assert "Folder 'path/to/file.csv' does not exists" == str(err)
