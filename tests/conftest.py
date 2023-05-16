import csv
import boto3
import pytest
import tempfile
import logging
from typing import Iterator
from botocore.config import Config
from tiny_blocks.base import Row
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy_utils import create_database
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import drop_database


logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def source_data() -> Iterator[Row]:
    data = [
        {"name": "Mateo", "age": 30},
        {"name": "Marcos", "age": 31},
        {"name": "Lucas", "age": 32},
        {"name": "Juan", "age": 33},
    ]
    return (Row(row) for row in data)


@pytest.fixture(scope="function")
def csv_source(source_data) -> tempfile.NamedTemporaryFile:
    """
    Yield a CSV Source with a path to an existing CSV file
    """

    with tempfile.NamedTemporaryFile(
        encoding="utf-8", suffix=".csv"
    ) as file, open(file.name, mode="w", newline="") as csvfile:

        # from the first row we get the column names for fieldnames
        first_row = next(source_data)
        writer = csv.DictWriter(csvfile, fieldnames=first_row.columns())
        writer.writeheader()

        # write the data, including the first row
        writer.writerow(first_row)
        for row in source_data:
            writer.writerow(row)

        yield file


@pytest.fixture(scope="function")
def csv_sink():
    """
    Yield a CSV Sink with a path to an existing CSV file
    """
    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        yield file.name


@pytest.fixture(scope="function")
def minio_config() -> Config:
    config = Config(
        aws_access_key_id="access-key",
        aws_secret_access_key="secret-key",
        retries={"max_attempts": 10, "mode": "standard"},
    )
    yield config


@pytest.fixture(scope="function")
def s3_source(csv_source, minio_config):
    """
    Yield a s3 object
    """
    s3 = boto3.resource("s3", **minio_config)
    bucket = s3.Bucket("my-bucket")

    if not bucket.creation_date:  # create if not exists
        s3.create_bucket(Bucket="my-bucket")

    bucket.upload_file(csv_source.name, "source.csv")  # overwrite
    yield s3.Object(bucket.name, "source.csv")  # yield the s3 object
    bucket.delete()


@pytest.fixture(scope="function")
def postgres_uri():
    """
    Sqlalchemy URI for Postgres DB Connection
    """
    yield "postgresql+psycopg2://user:pass@postgres:5432/db"


@pytest.fixture(scope="function")
def postgres_db(postgres_uri):
    """
    Creates a database
    """
    if database_exists(postgres_uri):
        drop_database(postgres_uri)
    create_database(postgres_uri)
    yield
    drop_database(postgres_uri)


@pytest.fixture(scope="function")
def postgres_conn(postgres_db, postgres_uri) -> Connection:
    """
    Yield a Postgres connection having previously created a Database
    """
    with create_engine(postgres_uri).connect() as conn:  # open transaction
        conn.execution_options(stream_results=True, autocommit=True)
        yield conn


@pytest.fixture(scope="function")
def postgres_source(postgres_conn) -> Connection:
    """
    Yield an SQL Source with a connection string to an existing Table DB
    """
    stm = "CREATE TABLE Users (name varchar(255), active int, created date);"
    postgres_conn.execute(statement=text(stm))

    stm = (
        "INSERT INTO Users (name, active, created) VALUES "
        "(Mateo, 1, 2022-01-10) "
        "(Marcos, 1, 2022-02-14) "
        "(Lucas, 0, 2022-03-08) "
        "(Juan, 0, 2022-04-05);"
    )
    postgres_conn.execute(statement=text(stm))

    yield postgres_conn
