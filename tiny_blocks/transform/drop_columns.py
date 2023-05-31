import logging
import itertools

from pydantic import Field, BaseModel
from typing import Iterator, Literal, List
from tiny_blocks.transform.base import TransformBase


__all__ = ["DropColumns"]


logger = logging.getLogger(__name__)


class DropColumns(TransformBase):
    """
    Drop Columns Block. Defines the drop columns functionality

    Basic example:
        >>> from tiny_blocks.transform import DropColumns
        >>> from tiny_blocks.extract import FromCSV
        >>>
        >>> extract_csv = FromCSV(path='/path/to/file.csv')
        >>> drop_na = DropColumns(columns=["Column A"])
        >>>
        >>> generator = extract_csv.get_iter()
        >>> generator = drop_na.get_iter(generator)
    """

    name: Literal["drop_columns"] = Field(default="drop_columns")
    columns: List[str] = Field(description="Columns to be dropped")

    def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:

        with tempfile.NamedTemporaryFile(suffix=".sqlite") as file:

            first_row = next(source)
            model = first_row.__class__

            class SortTable(first_row.__class__, SQLModel, table=True):
                pass

            # create table
            engine = create_engine(file.name, echo=True)
            SQLModel.metadata.create_all(engine)

            # write records in sqlite
            with Session(engine) as session:
                for row in itertools.chain([first_row], source):
                    session.add(SortTable(**row.dict()))
                session.commit()

                # Sort by
                order = "asc" if self.ascending else "desc"
                group_by = ", ".join(
                    [f"{column} {order}" for column in self.by]
                )
                statement = select(SortTable).group_by(text(group_by))
                for row in session.exec(statement).fetchmany(size=1000):
                    yield model(**row.dict())
