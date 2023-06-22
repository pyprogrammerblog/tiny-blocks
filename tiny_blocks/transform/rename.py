import copy
import logging
import itertools

from pydantic import Field, BaseModel
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
        >>> sort = Rename(columns={"column_name": "new_column_name"})
        >>>
        >>> generator = from_csv.get_iter()
        >>> generator = sort.get_iter(generator)
    """

    name: Literal["rename"] = Field(default="Rename")
    columns: Dict[str, str] = Field(description="Mapping dictionary")

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:

        # extract first row to retrieve the model
        first_row = next(source)
        input_model = first_row.__class__

        # generate an output model and yield the rows
        output_model = self._output_model(input_model=input_model)
        for row in itertools.chain([first_row], source):
            yield output_model(**row.dict())

    def _output_model(self, input_model: Type[BaseModel]) -> Type[BaseModel]:

        if missing := set(input_model.__fields__.keys()) - set(
            self.columns.keys()
        ):
            raise ValueError(f"Fields {','.join(missing)} not found.")

        output_model = copy.deepcopy(input_model)
        output_model.__name__ = f"Output_{self.name}_{self.uuid}"

        for new_key, old_key in self.columns:
            output_model.__fields__[new_key] = output_model.__fields__[old_key]

        return output_model
