import copy
import logging
import itertools

from pydantic import Field, BaseModel
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

        # extract first row to retrieve the model
        first_row = next(source)
        input_model = first_row.__class__

        # generate an output model and yield the rows
        output_model = self._output_model(input_model=input_model)
        for row in itertools.chain([first_row], source):
            yield output_model(**row.dict())

    def _output_model(self, input_model: Type[BaseModel]) -> Type[BaseModel]:

        output_model = copy.deepcopy(input_model)
        output_model.__name__ = f"Output_{self.name}_{self.uuid}"
        [output_model.__fields__.pop(column) for column in self.columns]

        return output_model
