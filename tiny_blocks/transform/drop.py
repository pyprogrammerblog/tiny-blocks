import logging
from typing import Iterator, Literal, List

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Drop", "KwargsDrop"]


logger = logging.getLogger(__name__)


class KwargsDrop(KwargsTransformBase):
    """
    More info:

    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
    """

    errors: Literal["ignore", "raise"] = "ignore"


class Drop(TransformBase):
    """
    Drop Columns Block. Defines the drop columns functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Drop
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = Drop(columns=["a"])
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
        >>> df = pd.concat(generator)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
    """

    name: Literal["drop_na"] = "drop_na"
    kwargs: KwargsDrop = KwargsDrop()
    columns: List[str]

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        for chunk in source:
            chunk = chunk.drop(columns=self.columns, **self.kwargs.to_dict())
            yield chunk
