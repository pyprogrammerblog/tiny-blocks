import itertools
import logging

from dataclasses import dataclass, make_dataclass
from pydantic import Field, BaseModel, create_model
from typing import Iterator, Literal, Set, Type
from tiny_blocks.load.base import LoadBase
from sqlmodel import Session, SQLModel, create_engine


__all__ = ["ToSQL"]


logger = logging.getLogger(__name__)


class ToSQL(LoadBase):
    """
    Load SQL Block. Defines the Loading operation to an SQL Database

    Basic example:
        >>> from tiny_blocks.extract import FromSQL
        >>> from tiny_blocks.load import ToSQL
        >>>
        >>> str_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> from_sql = FromSQL(dsn_conn=str_conn, query="select * from source")
        >>> to_sql = ToSQL(dsn_conn=str_conn, table="sink")
        >>>
        >>> generator = from_sql.get_iter()
        >>> to_sql.exhaust(generator)
    """

    def __init__(self, dsn_conn: str, table: str, columns: Set[str], **kwargs):
        super().__init__(**kwargs)

        self.dsn_conn = dsn_conn
        self.table = table
        self.columns = columns

    def exhaust(self, source: Iterator[BaseModel]):
        """
        Exhaust
        """
        # get the data model from the first row
        # use first row for extracting fieldnames
        try:
            first_row = next(source)
        except StopIteration:
            raise ValueError(f"Source is empty. No data to write.")

        # create a 'write_model'
        source_columns = set(first_row.__fields__.keys())
        if self.columns and (not_exist := self.columns - source_columns):
            raise ValueError(f"Not found: {', '.join(not_exist)}")
        elif self.columns:
            write_model = first_row.__class__
        else:
            write_model = first_row.__class__

        # create SQL Model from the 'write_model'
        class SQLRowModel(write_model, SQLModel):
            __tablename__ = self.table

        # open session, write records, and commit.
        with Session(create_engine(self.dsn_conn)) as session:
            for row in itertools.chain([first_row], source):
                session.add(instance=SQLRowModel(**row.dict()))
            session.commit()
