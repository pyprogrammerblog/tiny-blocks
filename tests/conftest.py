import pytest
import tempfile
from sqlalchemy import create_engine, text
from sqlalchemy_utils import create_database, database_exists, drop_database
import csv


@pytest.fixture(scope="function")
def csv_source():
    """
    Yield a CSV Source with a path to an existing CSV file
    """
    data = [
        {"name": "Albert", "age": 30},
        {"name": "Jaap", "age": 31},
        {"name": "Casper", "age": 32},
        {"name": "Jeroen", "age": 33},
    ]

    with tempfile.NamedTemporaryFile(encoding="utf-8") as file, open(
        file.name, "w", newline=""
    ) as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=["name", "age"])
        writer.writeheader()

        for row in data:
            writer.writerow(row)


@pytest.fixture(scope="function")
def csv_sink():
    """
    Yield a CSV Sink with a path to an existing CSV file
    """
    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        yield file.name


@pytest.fixture(scope="function")
def postgres_uri():
    yield "postgresql+psycopg2://user:pass@postgres:5432/db"


@pytest.fixture(scope="function")
def postgres_db(postgres_uri):
    if database_exists(postgres_uri):
        drop_database(postgres_uri)
    create_database(postgres_uri)
    yield
    drop_database(postgres_uri)


@pytest.fixture(scope="function")
def postgres_conn(postgres_db, postgres_uri):

    with create_engine(postgres_uri).begin() as conn:  # open transaction
        conn.execution_options(stream_results=True, autocommit=True)
        yield conn


@pytest.fixture(scope="function")
def postgres_source(postgres_db, postgres_conn, postgres_uri):
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """

    stm = "CREATE TABLE Users (name varchar(255), active int, created date);"
    postgres_conn.execute(statement=text(stm))

    statement = (
        "INSERT INTO Users(name, active, created) VALUES "
        "(Albert, 1, 2022-01-10) "
        "(Jaap, 1, 2022-02-14) "
        "(Casper, 0, 2022-03-08) "
        "(Jeroen, 0, 2022-04-05);"
    )
    postgres_conn.execute(statement=text(statement))
