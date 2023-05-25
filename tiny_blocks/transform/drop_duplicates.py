import logging
import sqlite3
import tempfile
from typing import Iterator, Literal
from tiny_blocks.transform.base import TransformBase
from typing import Optional, List
from pydantic import BaseModel
from sqlmodel import Field, SQLModel


__all__ = ["DropDuplicates"]


logger = logging.getLogger(__name__)


class DropDuplicates(TransformBase):
    """
    Drop Duplicates Block. Defines the drop duplicates functionality

    Basic example:
        >>> from tiny_blocks.transform import DropDuplicates
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_duplicates = DropDuplicates()
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_duplicates.get_iter(generator)
    """

    name: Literal["drop_duplicates"] = "drop_duplicates"
    keep: Literal["first", "last"] | None = "first"
    subset: List[str] = Field(default_factory=list)

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:

        with tempfile.NamedTemporaryFile(
            suffix=".sqlite"
        ) as file, sqlite3.connect(file.name) as con:

            first_row = next(source)

            # send records to a temp database (exhaust the generator)
            for row in source:
                row.to_sql(name="temp", con=con, index=False)

            # select non-duplicated rows. It is also possible to select
            # a non-duplicated subset of rows.
            by = ", ".join(self.subset)

            # yield records now without duplicates
            for row in con.execute(sql=f"select * from temp group by {by}"):
                yield row
