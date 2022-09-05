import logging
from typing import Iterator, Literal, List

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["DropNa", "KwargsDropNa"]


logger = logging.getLogger(__name__)


class KwargsDropNa(KwargsTransformBase):
    """
    Kwargs for DropNa
    """

    subset: str | List[str] = None
    axis: Literal["index", "columns"] = None
    how: Literal["any", "all"] = None
    thresh: int = None


class DropNa(TransformBase):
    """
    Drop Nan Block. Defines the drop None values functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropNa
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = DropNa()
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
        >>> df = pd.concat(generator)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dropna.html
    """

    name: Literal["drop_na"] = "drop_na"
    kwargs: KwargsDropNa = KwargsDropNa()

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        for chunk in source:
            chunk = chunk.dropna(**self.kwargs.to_dict())
            yield chunk
