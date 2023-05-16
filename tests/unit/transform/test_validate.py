from tiny_blocks.transform.validate import Validate
from pydantic import BaseModel, Field, ValidationError


class LazyValidationModel(BaseModel):
    name: str
    age: int


class ExtrictValidationModel(BaseModel):
    name: str = Field(max_length=3)
    age: int = Field(lt=20)


def test_validate_lazy(source_data):

    validate = Validate(model=LazyValidationModel)
    generator = validate.get_iter(source=source_data)

    # exhaust the generator (no errors)
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert data[0].columns() == ["name", "age"]
    assert data[0].values() == ["Mateo", 30]


def test_validate_extrict(source_data):

    validate = Validate(model=ExtrictValidationModel)
    generator = validate.get_iter(source=source_data)

    # exhaust the generator and check for validation errors
    try:
        list(generator)
    except ValidationError as e:
        assert isinstance(e.model, ExtrictValidationModel)
        assert isinstance(e.errors, list)
        assert e.errors[0] == 1
