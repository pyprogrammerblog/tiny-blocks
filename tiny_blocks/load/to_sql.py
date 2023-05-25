import itertools
import logging
from pydantic import Field, BaseModel
from typing import Iterator, Literal
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

    name: Literal["to_sql"] = Field(default="to_sql")
    dsn_conn: str = Field(..., description="Connection string")
    table_name: str = Field(..., description="Table name")

    def exhaust(self, source: Iterator[BaseModel]):
        """
        - Connect to DB and yield a transaction
        - Loop the source and send each chunk to SQL
        """
        first_row = next(source)

        class SQLRowModel(first_row.__class__, SQLModel):
            __tablename__ = self.table_name

        with Session(create_engine(self.dsn_conn)) as session:
            for row in itertools.chain([first_row], source):
                session.add(SQLRowModel(**row.dict()))
            session.commit()
