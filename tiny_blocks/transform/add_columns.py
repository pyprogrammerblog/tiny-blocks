import logging
import itertools

from pydantic import Field, BaseModel, create_model
from typing import Dict, Iterator, Literal, Type, Any
from tiny_blocks.transform.base import TransformBase


__all__ = ["AddColumns"]


logger = logging.getLogger(__name__)


class AddColumns(TransformBase):
    """
    AddColumns Block. Defines Rename columns functionality

    Basic example:
    """

    name: Literal["rename"] = Field(default="Rename")
    field_value_mapping: Dict[str, Any] = Field(
        description="Mapping dictionary"
    )

    def output_model(self, input_model: Type[BaseModel]) -> Type[BaseModel]:
        """
        This method takes an input model as a parameter
        and returns a new output model.
        """
        output_model = create_model(
            __model_name=f"{input_model}_output",
            __base__=input_model.__model__,
            **self.field_value_mapping,
        )
        return output_model

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:
        """
        Returns an iterator that transforms the input iterator
        """
        first_row = next(source)
        input_model = first_row.__class__
        output_model = self.output_model(input_model=input_model)

        for row in itertools.chain([first_row], source):
            yield output_model(**row.model_dump())
