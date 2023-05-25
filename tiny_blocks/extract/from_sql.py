import logging
from pydantic import BaseModel
from typing import Iterator, Literal, Type
from sqlmodel import Field, Session, SQLModel, create_engine, select
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

    name: Literal["read_sql"] = Field(default="read_sql")
    dsn_conn: str = Field(..., description="Connection string")
    row_model: Type[BaseModel] = Field(..., description="Row model")
    size: int = Field(default=1000, description="Chunk size")

    def get_iter(self) -> Iterator[BaseModel]:
        class SQLRowModel(self.row_model, SQLModel):
            pass

        with Session(create_engine(self.dsn_conn)) as session:
            statement = select(SQLRowModel())
            while rows := session.exec(statement).fetchmany(size=self.size):
                for row in rows:
                    yield self.row_model(**row.dict())
