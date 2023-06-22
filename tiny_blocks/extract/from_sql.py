import logging

from typing import Iterator
from pydantic import BaseModel, ValidationError
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
        dsn_conn: str,
        table: str,
        batch_size: int = 1000,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.dsn_conn = dsn_conn
        self.table = table
        self.size = batch_size

    def get_iter(self) -> Iterator[BaseModel]:
        class SQLRowModel(self.row_model, SQLModel, table=True):
            __table_args__ = {"extend_existing": True}
            __tablename__ = self.table

        collector = []

        with Session(create_engine(self.dsn_conn)) as session:
            statement = select(SQLRowModel)
            while rows := session.exec(statement).fetchmany(size=self.size):
                for row in rows:
                    try:
                        yield self.row_model(**row.dict())
                    except ValidationError as errs:
                        if not self.lazy_validation:
                            raise errs
                        collector.append(errs.errors())

            if collector:
                raise ValidationError(errors=collector, model=self.row_model)
