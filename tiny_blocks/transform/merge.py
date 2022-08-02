import logging
import pandas as pd
from sqlite3 import connect
from tempfile import TemporaryFile
from typing import Literal, Iterator
from tiny_blocks.transform.base import (
    KwargsTransformBase,
    TransformBase,
)


__all__ = ["KwargsMerge", "MergeBlock"]

logger = logging.getLogger(__name__)


class KwargsMerge(KwargsTransformBase):
    """
    Kwargs for block merge
    """

    how: Literal["left", "right", "outer", "inner", "cross"] = "inner"
    left_on: str
    right_on: str
    chunksize: int = 1000


class MergeBlock(TransformBase):
    """
    Merge Block
    """

    name: Literal["merge"] = "merge"
    kwargs: KwargsMerge

    def process(
        self,
        left_generator: Iterator[pd.DataFrame],
        right_generator: Iterator[pd.DataFrame],
    ) -> Iterator[pd.DataFrame]:
        """
        Drop Duplicates
        """
        with TemporaryFile(suffix=".sqlite") as file, connect(file) as con:

            # send records to a temp database (exhaust the generators)
            for chunk in left_generator:
                chunk.to_sql(name="TEMP_TABLE_LEFT", con=con)

            for chunk in right_generator:
                chunk.to_sql(name="TEMP_TABLE_RIGHT", con=con)

            # select non-duplicated rows.
            # It is possible select a non-duplicated subset of rows.
            sql = (
                f"SELECT * FROM TEMP_TABLE_LEFT "
                f"{self.kwargs.how} JOIN TEMP_TABLE_RIGHT "
                f"ON TEMP_TABLE_LEFT.{self.kwargs.left_on}"
                f" = TEMP_TABLE_RIGHT.{self.kwargs.right_on}"
            )

            # yield joined records
            chunk = self.kwargs.chunksize
            for chunk in pd.read_sql_query(con=con, sql=sql, chunksize=chunk):
                yield chunk
