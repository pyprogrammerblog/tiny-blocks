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
    Validate block. Defines block to apply validation.

    When `lazy=True` all errors are collected and raised at the end.

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

        collector = pd.DataFrame()

        for chunk in source:
            try:
                yield self.schema_model.validate(chunk, lazy=self.lazy)
            except pa.errors.SchemaErrors as errs:
                collector = pd.concat([collector, errs.failure_cases])

        if not collector.empty:
            raise SchemaErrors(collector.to_json(orient="records"))
