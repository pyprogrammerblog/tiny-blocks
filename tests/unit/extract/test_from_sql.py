import datetime
from tests.conftest import Hero
from tiny_blocks.extract.from_sql import FromSQL
from pydantic import BaseModel


def test_extract_from_sql(postgres_source, postgres_uri):

    from_sql = FromSQL(row_model=Hero, dsn_conn=postgres_uri, table="Hero")

    # exhaust the generator
    generator = from_sql.get_iter()
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert list(data[0].dict().keys()) == [
        "name",
        "secret_name",
        "age",
        "created",
    ]


# def test_extract_from_sql_extrict_row_validation(csv_source):
#
#     class MyRowModel(BaseModel):
#         name: str = Field(max_length=2)
#         age: int = Field(lt=10)
#
#     from_sql = FromCSV(path=csv_source.name, row_model=MyRowModel)
#     generator = from_sql.get_iter()
#
#     # exhaust the generator to get validation errors
#     try:
#         list(generator)
#     except ValidationError as error:
#         assert len(error.errors()) == 2
#         assert "2 validation errors for MyRowModel" in str(error)
#
#
# def test_extract_from_sql_path_exists(csv_source):
#
#     class MyRowModel(BaseModel):
#         name: str = Field(max_length=2)
#         age: int = Field(lt=10)
#
#     try:
#         FromCSV(path=Path('path/to/file.csv'), row_model=MyRowModel)
#     except ValueError as err:
#         assert "Folder 'path/to/file.csv' does not exists" == str(err)
