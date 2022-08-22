import logging
from typing import Iterator, Literal, Union

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Fillna", "KwargsFillNa"]


logger = logging.getLogger(__name__)


class KwargsFillNa(KwargsTransformBase):
    """
    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html
    """

    method: Literal["backfill", "bfill", "pad", "ffill"] = None
    limit: int = None
    axis: int = None


class Fillna(TransformBase):
    """
    Fill Nan Block. Defines the fill Nan values functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> fill_na = Fillna(value="Hola Mundo")
        >>> source = extract_csv.get_iter()
        >>> generator = fill_na.get_iter(source)
        >>> df = pd.concat(generator)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.fillna.html
    """

    name: Literal["fillna"] = "fillna"
    kwargs: KwargsFillNa = KwargsFillNa()
    value: Union[int, str, dict]

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        1. Loop on each chunk.
        2. Fill Nan values
        3. Yield chunk
        """

        for chunk in source:
            chunk = chunk.fillna(value=self.value, **self.kwargs.to_dict())
            yield chunk
