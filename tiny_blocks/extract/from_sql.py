import logging

from typing import Iterator, Type
from pydantic import BaseModel
from sqlmodel import Session, SQLModel, create_engine, select
from tiny_blocks.extract.base import ExtractBase


__all__ = ["FromSQL"]


logger = logging.getLogger(__name__)


class FromSQL(ExtractBase):
    """
    Read SQL Query Block. Defines the read SQL Query Operation

    Basic example:
        >>> from tiny_blocks.extract import FromSQL
        >>>
        >>> dsn_conn = "postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> read_sql = FromSQL(
        >>> ... dsn_conn="postgresql+psycopg2://...",
        >>> ... row_model="select * from test",
        >>> )
        >>> generator = read_sql.get_iter()
    """

    def __init__(
        self,
        row_model: Type[BaseModel],
        dsn_conn: str,
        table: str,
        batch_size: int = 1000,
    ):
        self.row_model = row_model
        self.dsn_conn = dsn_conn
        self.table = table
        self.size = batch_size

    def get_iter(self) -> Iterator[BaseModel]:
        class SQLRowModel(self.row_model, SQLModel):
            __tablename__ = self.table

        with Session(create_engine(self.dsn_conn)) as session:
            statement = select(SQLRowModel())
            while rows := session.exec(statement).fetchmany(size=self.size):
                for row in rows:
                    yield self.row_model(**row.dict())
