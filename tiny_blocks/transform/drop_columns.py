import logging
import itertools

from pydantic import Field, BaseModel, create_model
from typing import Iterator, Literal, List, Type
from tiny_blocks.transform.base import TransformBase


__all__ = ["DropColumns"]


logger = logging.getLogger(__name__)


class DropColumns(TransformBase):
    """
    Drop Columns Block. Defines the drop columns functionality

    Basic example:
        >>> from tiny_blocks.transform import DropColumns
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = DropColumns(columns=["Column A"])
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
    """

    name: Literal["drop_columns"] = Field(default="drop_columns")
    columns: List[str] = Field(description="Columns to be dropped")

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:

        first_row = next(source)
        input_model = first_row.__class__
        output_model = self.output_model(input_model=input_model)

        for row in itertools.chain([first_row], source):
            yield output_model(**row.model_dump())

    def output_model(self, input_model: Type[BaseModel]) -> Type[BaseModel]:

        model_fields = set(input_model.model_fields)
        if not_found := set(self.columns) - model_fields:
            logger.warning(
                f"Mapping fields {','.join(not_found)} not found "
                f"in model fields {','.join(model_fields)}. "
                f"Mapping fields {','.join(set(self.columns))}"
            )

        new_model_fields = {
            name: (field.outer_type_, field.default)
            for name, field in input_model.__annotations__.items()
            if name not in self.columns
        }

        output_model = create_model(
            __model_name=f"{input_model}_output", **new_model_fields
        )
        return output_model
