import logging
from typing import Dict, Iterator, Literal

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Astype", "KwargsAstype"]


logger = logging.getLogger(__name__)


class KwargsAstype(KwargsTransformBase):
    """
    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
    """

    errors: Literal["raise", "ignore"] = "ignore"


class Astype(TransformBase):
    """
    Astype Block. Defines the casting of types for dataframes or columns

    Basic Usage:
        >>> from tiny_blocks.transform import Astype
        >>> from tiny_blocks.extract import FromCSV
        >>> extract_sql = FromCSV(path="/path/to/file.csv")
        >>> as_type = Astype(dtype={"e": "float32"})
        >>> generator = extract_sql.get_iter()
        >>> generator = as_type.get_iter(generator=generator)
        >>> df = pd.concat(generator)
        >>> assert not df.empty

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
    """

    name: Literal["astype"] = "astype"
    dtype: Dict[str, str]
    kwargs: KwargsAstype = KwargsAstype()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Cast types
        """
        for chunk in generator:
            chunk = chunk.astype(dtype=self.dtype, **self.kwargs.to_dict())
            yield chunk
