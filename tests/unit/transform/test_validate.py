import pandas as pd
import pandera as pa
import pytest
from pandera.typing import Series
from tiny_blocks.extract.from_csv import FromCSV
from tiny_blocks.transform.validate import Validate
from tiny_blocks.transform.validate import SchemaErrors
from pandera.errors import SchemaError


class SchemaModel(pa.SchemaModel):
    a: Series[int]
    b: Series[int] = pa.Field(le=5)
    c: Series[int] = pa.Field(le=3)


def test_validate_lazy_true(csv_source):

    from_csv = FromCSV(path=csv_source, kwargs={"chunksize": 1})
    validate = Validate(schema_model=SchemaModel, lazy=True)

    generator = from_csv.get_iter()
    generator = validate.get_iter(source=generator)

    try:
        pd.concat(generator)
    except SchemaErrors as e:
        errors_df = pd.read_json(str(e))
        assert not errors_df.empty
        assert errors_df.shape == (6, 6)  # 6 errors in total


def test_validate_lazy_false(csv_source):

    from_csv = FromCSV(path=csv_source, kwargs={"chunksize": 1})
    validate = Validate(schema_model=SchemaModel, lazy=False)

    generator = from_csv.get_iter()
    generator = validate.get_iter(source=generator)

    with pytest.raises(SchemaError):
        pd.concat(generator)
