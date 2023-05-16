import logging
import sqlite3
import tempfile
from typing import Iterator, Literal, List
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.base import Row


__all__ = ["Sort"]


logger = logging.getLogger(__name__)


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
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = sort.get_iter(generator)
        >>> df = pd.concat(generator)
    """

    name: Literal["sort"] = "sort"
    by: List[str]
    ascending: bool = True

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

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

            # yield sorted records
            for row in con.execute(sql):
                yield Row(row)
