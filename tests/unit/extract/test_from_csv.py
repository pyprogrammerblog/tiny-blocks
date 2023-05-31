from tiny_blocks.extract.from_csv import FromCSV
from pydantic import BaseModel, Field, ValidationError
from pathlib import Path


def test_extract_from_csv(csv_source):
    class MyRowModel(BaseModel):
        name: str
        age: int

    from_csv = FromCSV(path=csv_source.name, row_model=MyRowModel)
    generator = from_csv.get_iter()

    # exhaust the generator
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert list(data[0].dict().keys()) == ["name", "age"]
    assert list(data[0].dict().values()) == ["Mateo", 30]


def test_extract_from_csv_extrict_row_validation(csv_source):
    class MyRowModel(BaseModel):
        name: str = Field(max_length=2)
        age: int = Field(lt=10)

    from_csv = FromCSV(path=csv_source.name, row_model=MyRowModel)
    generator = from_csv.get_iter()

    # exhaust the generator to get validation errors
    try:
        list(generator)
    except ValidationError as error:
        assert len(error.errors()) == 2
        assert "2 validation errors for MyRowModel" in str(error)


def test_extract_from_csv_path_exists(csv_source):
    class MyRowModel(BaseModel):
        name: str = Field(max_length=2)
        age: int = Field(lt=10)

    try:
        FromCSV(path=Path("path/to/file.csv"), row_model=MyRowModel)
    except ValueError as err:
        assert "Folder 'path/to/file.csv' does not exists" == str(err)
