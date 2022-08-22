import logging
import sqlite3
import tempfile
from typing import Iterator, Literal, List

import pandas as pd
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["Sort", "KwargsSort"]


logger = logging.getLogger(__name__)


class KwargsSort(KwargsTransformBase):
    """
    Kwargs for DropDuplicates
    """

    chunksize: int = 1000


class Sort(TransformBase):
    """
    Sort Block. Defines the Sorting operation

    Basic example:
        >>> import pandas as pd
        >>> from tiny_blocks.transform import Sort
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> sort = Sort(by=["column_A"], ascending=False)
        >>> source = extract_csv.get_iter()
        >>> generator = sort.get_iter(source)
        >>> df = pd.concat(generator)
    """

    name: Literal["sort"] = "sort"
    by: List[str]
    ascending: bool = True
    kwargs: KwargsSort = KwargsSort()

    def get_iter(
        self, source: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        with tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as file, sqlite3.connect(file.name) as con:

            # send records to a temp database (exhaust the generator)
            for chunk in source:
                chunk.to_sql(name="temp", con=con, index=False)

            # order by column(s) ascending/descending.
            sql = f"""
            SELECT * FROM temp
            ORDER BY {", ".join(self.by)}
            {'ASC' if self.ascending else 'DESC'}
            """

            # yield records now without duplicates
            chunk = self.kwargs.chunksize
            for chunk in pd.read_sql_query(con=con, sql=sql, chunksize=chunk):
                yield chunk
