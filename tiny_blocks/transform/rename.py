import logging
import itertools

from pydantic import Field, BaseModel
from typing import Dict, Iterator, Literal
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

        # Create an output model
        first_row = next(source)
        input_model = first_row.__class__
        output_model = ...

        # Output regenerated data
        for row in itertools.chain([first_row], source):
            yield output_model(**row.dict())
