from pydantic import Field
from typing import Iterator, Literal, List
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row

import itertools
import logging


__all__ = ["DropNone"]


logger = logging.getLogger(__name__)


class DropNone(TransformBase):
    """
    Drop Nan Block. Defines the drop None values functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropNone
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_none = DropNone()
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_none.get_iter(generator)
    """

    name: Literal["drop_none"] = "drop_none"
    subset: List[str] = Field(default_factory=list)

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        # check the subset exists in the source
        first_row = next(source)
        if missing_columns := set(first_row.columns()) - set(self.subset):
            raise ValueError(f"'{', '.join(missing_columns)}' do not exist")

        # drop None values
        for row in itertools.chain([first_row], source):
            for key, value in row.items():
                if self.subset and key not in self.subset:
                    continue
                if value is None:
                    del key
            yield row
