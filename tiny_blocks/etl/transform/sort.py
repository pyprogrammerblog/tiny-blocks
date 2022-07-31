import logging
import pandas as pd
from sqlite3 import connect
from tempfile import TemporaryFile
from typing import Literal, Iterator, Set
from tiny_blocks.etl.transform.base import (
    KwargsTransformBase,
    TransformBase,
)

__all__ = ["DropDuplicatesBlock", "KwargsDropDuplicates"]


logger = logging.getLogger(__name__)


class KwargsDropDuplicates(KwargsTransformBase):
    """
    Kwargs for DropDuplicatesBlock
    """

    chunksize: int = 1000
    subset: Set[str] = {}


class DropDuplicatesBlock(TransformBase):
    """
    Operator DropDuplicatesBlock
    """

    name: Literal["drop_duplicates"] = "drop_duplicates"
    kwargs: KwargsDropDuplicates = KwargsDropDuplicates()

    def process(
        self, generator: Iterator[pd.DataFrame]
    ) -> Iterator[pd.DataFrame]:
        """
        Drop Duplicates
        """
        with TemporaryFile(suffix=".sqlite") as file, connect(file) as con:

            # send records to a temp database (exhaust the generator)
            for chunk in generator:
                chunk.to_sql(name="TEMP_TABLE", con=con)

            # select non-duplicated rows.
            # It is possible select a non-duplicated subset of rows.
            sql = (
                f"SELECT * FROM temp "
                f"WHERE rowid not in "
                f"(SELECT MIN(rowid) from TEMP_TABLE "
                f"GROUP BY {'*' or self.kwargs.subset})"
            )

            # yield records now without duplicates
            chunk = self.kwargs.chunksize
            for chunk in pd.read_sql_query(con=con, sql=sql, chunksize=chunk):
                yield chunk
