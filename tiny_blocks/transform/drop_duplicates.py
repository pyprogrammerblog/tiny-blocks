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
    subset: Set[str] = {}


class DropDuplicates(TransformBase):
    """
    Drop Duplicates Block. Defines the drop duplicates functionality
    """

    name: Literal["drop_duplicates"] = "drop_duplicates"
    kwargs: KwargsDropDuplicates = KwargsDropDuplicates()

    def get_iter(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:

        with tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as file, sqlite3.connect(file.name) as con:

            # send records to a temp database (exhaust the generator)
            for chunk in generator:
                chunk.to_sql(name="temp", con=con, index=False)

            # select non-duplicated rows. It is also possible to select
            # a non-duplicated subset of rows.
            sql = f"""
            SELECT * FROM temp
            WHERE rowid not in
            (SELECT MIN(rowid) from temp
            GROUP BY {", ".join(self.kwargs.subset) or "'*'"})
            """

            # yield records now without duplicates
            chunk = self.kwargs.chunksize
            for chunk in pd.read_sql_query(con=con, sql=sql, chunksize=chunk):
                yield chunk
