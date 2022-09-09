import logging
import pandas as pd
import pandera as pa
from typing import Literal, Iterator
from tiny_blocks.transform.base import TransformBase

__all__ = ["Validate", "SchemaErrors"]


logger = logging.getLogger(__name__)


class SchemaErrors(Exception):
    """
    Collect multiple errors. Lazy=True
    """

    pass


class Validate(TransformBase):
    """
    Validate block. Defines block to apply Pandera validation.
    When `lazy=True` all errors are collected and raised at the end.

    For more info:
    https://pandera.readthedocs.io/en/stable/schema_models.html

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Apply
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> validate = Validate(schema_model=pandera_schema, lazy=True)
        >>>
        >>> generator = from_csv.get_iter()
        >>> generator = validate.get_iter(generator)
        >>> df = pd.concat(generator)
    """

    name: Literal["validate"] = "validate"
    schema_model: pa.SchemaModel
    lazy: bool = True

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        failure_cases = pd.DataFrame()

        for chunk in source:
            try:
                yield self.schema_model.validate(chunk, lazy=self.lazy)
            except pa.errors.SchemaErrors as errs:
                failure_cases = pd.concat([failure_cases, errs.failure_cases])

        if not failure_cases.empty:
            raise SchemaErrors(failure_cases.to_json(orient="records"))
