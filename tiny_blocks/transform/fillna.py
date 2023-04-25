import logging
from typing import Iterator, Literal, Union
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row


__all__ = ["FillNone"]


logger = logging.getLogger(__name__)


class FillNone(TransformBase):
    """
    Fill None Block. Defines the fill Nan values functionality

    Basic example:
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> fill_na = FillNone(value="Hola Mundo")
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = fill_na.get_iter(generator)
    """

    name: Literal["fill_none"] = "fill_none"
    value: Union[int, str, dict]

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        for row in source:
            for key, value in row.items():
                if value is None:
                    row[key] = self.value
            yield row
