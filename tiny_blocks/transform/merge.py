import logging
from sqlite3 import connect
import tempfile
from typing import Iterator, Literal

import pandas as pd
from pydantic import Field
from tiny_blocks.transform.base import KwargsTransformBase, TransformBase

__all__ = ["KwargsMerge", "Merge"]

logger = logging.getLogger(__name__)


class KwargsMerge(KwargsTransformBase):
    """
    Kwargs for merge
    """

    chunksize: int = 1000


class Merge(TransformBase):
    """
    Merge. Defines merge functionality between two blocks.
    """

    name: Literal["merge"] = "merge"
    how: Literal["left", "right", "outer", "inner", "cross"] = "inner"
    left_on: str = Field(..., description="Column on the left table")
    right_on: str = Field(..., description="Column on the right table")
    kwargs: KwargsMerge = KwargsMerge()

    def get_iter(
        self,
        left: Iterator[pd.DataFrame],
        right: Iterator[pd.DataFrame],
    ) -> Iterator[pd.DataFrame]:

        with tempfile.NamedTemporaryFile(suffix=".sqlite") as file, connect(
            file.name
        ) as con:

            # send records to a temp database (exhaust the generators)
            for chunk in left:
                chunk.to_sql(name="table_left", con=con, index=False)

            for chunk in right:
                chunk.to_sql(name="table_right", con=con, index=False)

            # select non-duplicated rows.
            # It is possible select a non-duplicated subset of rows.
            sql = (
                f"SELECT * FROM table_left "
                f"{self.how.capitalize()} JOIN table_right "
                f"ON table_left.{self.left_on}"
                f" = table_right.{self.right_on}"
            )

            # yield joined records
            kwargs = self.kwargs.dict()
            for chunk in pd.read_sql_query(con=con, sql=sql, **kwargs):
                yield chunk
