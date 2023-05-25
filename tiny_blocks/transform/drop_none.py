from pydantic import Field, BaseModel
from typing import Iterator, Literal, List
from tiny_blocks.transform.base import TransformBase

import itertools
import logging


__all__ = ["DropNone"]


logger = logging.getLogger(__name__)


class DropNone(TransformBase):
    """
    Drop Nan Block. Defines the drop None values functionality

    Basic example:
        >>> from tiny_blocks.transform import DropNone
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop = DropNone()
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop.get_iter(generator)
    """

    name: Literal["drop_none"] = "drop_none"
    subset: List[str] = Field(default_factory=list)

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:

        # check the subset exists in the source
        first_row = next(source)
        if missing_columns := set(self.subset) - set(first_row.columns()):
            raise ValueError(f"'{', '.join(missing_columns)}' do not exist.")

        # fill none values
        for row in itertools.chain([first_row], source):
            for key, value in row.items():
                if self.subset and key not in self.subset:
                    continue
                if value is None:
                    row[key] = self.value
            yield row
