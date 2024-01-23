import csv
import pytest
import logging
import itertools

from sqlmodel import Field
from pydantic import BaseModel
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Iterator, Optional


logger = logging.getLogger(__name__)


class Hero(BaseModel):
    name: str
    secret_name: str
    age: Optional[int] = None
    created: datetime = Field(default_factory=datetime.now)


def source_data() -> Iterator[BaseModel]:
    data = [
        {"name": "Dive Wilson", "secret_name": "Deadpond", "age": 30},
        {"name": "Pedro Parqueador", "secret_name": "Spider-Boy", "age": 30},
        {"name": "Lucas Altos", "secret_name": "Wolf-Child", "age": 33},
        {"name": "Juan Benne", "secret_name": "La rana", "age": 33},
    ]
    for row in data:
        yield Hero(**row)


@pytest.fixture(scope="function")
def csv_source() -> NamedTemporaryFile:
    """
    Yield a CSV Source with a path to an existing CSV file
    """
    data = source_data()

    with NamedTemporaryFile(encoding="utf-8", suffix=".csv", mode="w") as file:

        with open(file.name, mode="w", newline="") as csvfile:
            # from the first row we get the column names for the header
            first_row = next(data)
            fieldnames = list(first_row.keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # write the data, including the first row
            for row in itertools.chain([first_row], data):
                writer.writerow(row.dict())

        yield file


@pytest.fixture(scope="function")
def csv_sink():
    """
    Yield a CSV Sink with a path to an existing CSV file
    """
    with NamedTemporaryFile(encoding="utf-8", suffix=".csv", mode="w") as file:
        yield file.name


# @pytest.fixture(scope="function")
# def s3_config() -> Config:
#     """
#     S3 Config
#     """
#     config = Config(
#         aws_access_key_id="access-key",
#         aws_secret_access_key="secret-key",
#         retries={"max_attempts": 10, "mode": "standard"},
#     )
#     yield config
#
#
# @pytest.fixture(scope="function")
# def s3_source(csv_source, s3_config):
#     """
#     Yield a s3 object
#     """
#     s3 = boto3.resource("s3", config=s3_config)
#     bucket = s3.Bucket("my-bucket")
#
#     if not bucket.creation_date:  # create if not exists
#         s3.create_bucket(Bucket="my-bucket")
#
#     bucket.upload_file(csv_source.name, "source.csv")  # overwrite
#     yield s3.Object(bucket.name, "source.csv")  # yield the s3 object
#     bucket.delete()
#
#
# @pytest.fixture(scope="function")
# def postgres_uri():
#     """
#     Sqlalchemy URI for Postgres DB Connection
#     """
#     yield "postgresql+psycopg2://user:pass@postgres:5432/db"
#
#
# @pytest.fixture(scope="function")
# def postgres_db(postgres_uri):
#     """
#     Creates a database
#     """
#     if database_exists(postgres_uri):
#         drop_database(postgres_uri)
#     create_database(postgres_uri)
#     yield
#     drop_database(postgres_uri)
#
#
# @pytest.fixture(scope="function")
# def postgres_source(postgres_db, postgres_uri):
#     """
#     Yield an SQL Source with a connection string to an existing Table DB
#     """
#     data = source_data()
#
#     class SQLHero(Hero, SQLModel, table=True):
#         id: Optional[int] = Field(default=None, primary_key=True)
#         __tablename__ = "Hero"
#
#     engine = create_engine(postgres_uri, echo=True)
#     SQLModel.metadata.create_all(engine)
#
#     with Session(engine) as session:
#         for row in data:
#             session.add(SQLHero(**row.dict()))
#             session.commit()
#
#     yield
