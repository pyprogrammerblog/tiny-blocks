import logging
from typing import Dict, Iterator, Literal

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Rename", "KwargsRename"]


logger = logging.getLogger(__name__)


class KwargsRename(KwargsTransformBase):
    """
    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
    """

    pass


class Rename(TransformBase):
    """
    Rename Block. Defines Rename columns functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Rename
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> sort = Rename(columns={"column_name": "new_column_name"})
        >>> source = from_csv.get_iter()
        >>> generator = sort.get_iter(source)
        >>> df = pd.concat(generator)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
    """

    name: Literal["rename"] = "rename"
    kwargs: KwargsRename = KwargsRename()
    columns: Dict[str, str]

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        for chunk in source:
            chunk = chunk.rename(columns=self.columns, **self.kwargs.to_dict())
            yield chunk
