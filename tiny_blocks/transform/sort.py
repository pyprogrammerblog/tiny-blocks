import logging
import sqlite3
import tempfile
from pydantic import Field, BaseModel
from typing import Iterator, Literal, List
from tiny_blocks.transform.base import TransformBase


__all__ = ["Sort"]


logger = logging.getLogger(__name__)


class Sort(TransformBase):
    """
    Sort Block. Defines the Sorting operation

    Basic example:
        >>> from tiny_blocks.transform import Sort
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> sort = Sort(by=["column_A"], ascending=False)
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = sort.get_iter(generator)
    """

    name: Literal["sort"] = Field(default="sort")
    by: List[str] = Field(description="Sorted by columns")
    ascending: bool = Field(default=True, description="Ascending/Descending")

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:

        with tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as file, sqlite3.connect(file.name) as con:

            first_row = next(source)

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
