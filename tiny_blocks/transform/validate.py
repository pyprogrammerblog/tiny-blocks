import logging
import pandas as pd
import pandera as pa
from typing import Literal, Iterator
from pydantic import Field
from tiny_blocks.transform.base import TransformBase

__all__ = ["Validate", "SchemaErrors"]


logger = logging.getLogger(__name__)


class SchemaErrors(Exception):
    """
    Collect multiple errors. Lazy=True
    """

    pass


class SchemaError(Exception):
    """
    Raised when first error found. Lazy=False
    """

    pass


class Validate(TransformBase):
    """
    Validate block. Defines block to apply validation.

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Apply
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> validate = Validate(
        ...   schema_model=my_schema_validation, lazy=True
        >>> )
        >>>
        >>> generator = from_csv.get_iter()
        >>> generator = validate.get_iter(generator)
        >>> df = pd.concat(generator)
    """

    name: Literal["validate"] = "validate"
    schema_model: pa.SchemaModel
    lazy: bool = Field(
        default=True,
        description="When set to True, collect all "
        "errors and raise them at the end",
    )

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        collector = pd.DataFrame()

        for chunk in source:
            try:
                self.schema_model.validate(chunk, lazy=self.lazy)
                yield chunk
            except pa.errors.SchemaErrors as errs:
                collector = pd.concat([collector, errs.failure_cases])

        if not collector.empty:
            raise SchemaErrors(collector.to_json(orient="records"))
