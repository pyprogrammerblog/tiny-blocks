import logging
from typing import Iterator, Literal, List

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["DropColumns", "KwargsDropColumns"]


logger = logging.getLogger(__name__)


class KwargsDropColumns(KwargsTransformBase):
    """
    More info:

    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
    """

    errors: Literal["ignore", "raise"] = "ignore"


class DropColumns(TransformBase):
    """
    Drop Columns Block. Defines the drop columns functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropColumns
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = DropColumns(columns=["a"])
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
        >>> df = pd.concat(generator)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
    """

    name: Literal["drop_columns"] = "drop_columns"
    kwargs: KwargsDropColumns = KwargsDropColumns()
    columns: List[str]

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        for chunk in source:
            chunk = chunk.drop(columns=self.columns, **self.kwargs.to_dict())
            yield chunk
