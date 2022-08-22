import logging
import sqlite3
import tempfile
from typing import Iterator, Literal, Set

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["DropDuplicates", "KwargsDropDuplicates"]


logger = logging.getLogger(__name__)


class KwargsDropDuplicates(KwargsTransformBase):
    """
    Kwargs for DropDuplicates
    """

    chunksize: int = 1000


class DropDuplicates(TransformBase):
    """
    Drop Duplicates Block. Defines the drop duplicates functionality

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import DropDuplicates
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_duplicates = DropDuplicates()
        >>> source = extract_csv.get_iter()
        >>> generator = drop_duplicates.get_iter(source)
        >>> df = pd.concat(generator)

    For more Kwargs info:
    https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html
    """

    name: Literal["drop_duplicates"] = "drop_duplicates"
    kwargs: KwargsDropDuplicates = KwargsDropDuplicates()
    subset: Set[str] = None

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        with tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as file, sqlite3.connect(file.name) as con:

            # send records to a temp database (exhaust the generator)
            for chunk in source:
                chunk.to_sql(name="temp", con=con, index=False)

            # select non-duplicated rows. It is also possible to select
            # a non-duplicated subset of rows.
            sql = f"""
            SELECT *
            FROM temp
            GROUP BY {", ".join(self.subset or chunk.columns.to_list())} ;
            """

            # yield records now without duplicates
            chunk = self.kwargs.chunksize
            for chunk in pd.read_sql_query(con=con, sql=sql, chunksize=chunk):
                yield chunk
