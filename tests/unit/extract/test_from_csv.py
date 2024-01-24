from tiny_blocks.extract.from_csv import FromCSV
from pydantic import ValidationError, Field
from tests.conftest import Hero
from pathlib import Path
import pytest


def test_extract_from_csv(csv_source):

    from_csv = FromCSV(path=Path(csv_source), row_model=Hero)
    generator = from_csv.get_iter()

    # exhaust the generator
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert isinstance(data[0], Hero)
    assert list(data[0].model_dump().keys()) == [
        "name",
        "age",
        "secret_name",
        "created",
    ]


def test_extract_from_csv_extrict_row_validation(csv_source):
    class AdultHero(Hero):
        age: int = Field(..., ge=18)

    from_csv = FromCSV(path=Path(csv_source), row_model=AdultHero)
    generator = from_csv.get_iter()

    # exhaust the generator to get validation errors
    with pytest.raises(ValidationError) as error:
        list(generator)

    assert len(error.errors()) == 2
    assert "2 validation errors for MyRowModel" in str(error)
