import logging
import itertools

from pydantic import Field, BaseModel, create_model
from typing import Dict, Iterator, Literal, Type
from tiny_blocks.transform.base import TransformBase


__all__ = ["Rename"]


logger = logging.getLogger(__name__)


class Rename(TransformBase):
    """
    Rename Block. Defines Rename columns functionality

    Basic example:
        >>> from tiny_blocks.transform import Rename
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> sort = Rename(fields_mapping={"column_name": "new_column_name"})
        >>>
        >>> from_csv = from_csv.get_iter()
        >>> rename = sort.get_iter(generator=from_csv)
        >>> next(rename)
    """

    name: Literal["rename"] = Field(default="Rename")
    fields_mapping: Dict[str, str] = Field(description="Mapping dictionary")

    def output_model(self, input_model: Type[BaseModel]) -> Type[BaseModel]:
        """
        This method takes an input model as a parameter
        and returns a new output model.
        """
        mapping_fields = set(self.fields_mapping.keys())
        model_fields = set(input_model.model_fields)
        if not_found := mapping_fields - model_fields:
            logger.warning(
                f"Mapping fields {','.join(not_found)} not found "
                f"in model fields {','.join(model_fields)}. "
                f"Mapping fields {','.join(mapping_fields)}"
            )

        output_model_fields = {}
        for old_name, new_name in self.fields_mapping.items():
            output_model_fields[new_name] = (
                input_model.__annotations__[old_name],
                input_model.model_fields[old_name].default,
            )

        output_model = create_model(
            __model_name=f"{input_model}_output",
            __base__=input_model.__model__,
            **output_model_fields,
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
            row_dict = row.model_dump()
            for old_name, new_name in self.fields_mapping.items():
                row_dict[new_name] = row_dict.pop(old_name)

            yield output_model(**row_dict)
