import logging
import itertools
from pydantic import Field
from typing import Iterator, Literal, Any, List
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row


__all__ = ["FillNone"]


logger = logging.getLogger(__name__)


class FillNone(TransformBase):
    """
    Fill None Block. Defines the fill Nan values functionality

    Basic example:
        >>> from tiny_blocks.transform import FillNone
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> fill_na = FillNone(value="Hola Mundo")
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = fill_na.get_iter(generator)
    """

    name: Literal["fill_none"] = "fill_none"
    value: Any = Field(description="Value to be filled")
    subset: List[str] = Field(default_factory=list)

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

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
