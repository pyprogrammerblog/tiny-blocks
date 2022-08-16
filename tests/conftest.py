import sqlite3
import tempfile

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists, drop_database


@pytest.fixture(scope="function")
def csv_source():
    """
    Yield a CSV Source with a path to an existing CSV file
    """
    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        data = {"a": [1, 3, 3, 3], "b": [4, 5, 6, 6], "c": [7, 8, 9, 9]}
        pd.DataFrame(data=data).to_csv(file.name, sep="|", index=False)
        yield file.name


@pytest.fixture(scope="function")
def csv_sink():
    """
    Yield a CSV Sink with a path to an existing CSV file
    """
    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        yield file.name


@pytest.fixture(scope="function")
def sqlite_source():
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """
    with tempfile.NamedTemporaryFile(suffix=".db") as file, sqlite3.connect(
        file.name
    ) as con:
        data = {"d": [1, 2, 3], "e": [4, 5, 6], "f": [7, 8, None]}
        pd.DataFrame(data=data).to_sql(name="TEST", con=con, index=False)
        yield f"sqlite:///{file.name}"


@pytest.fixture(scope="function")
def sqlite_sink():
    """
    Yield a SQL Sink with a connection string to an existing Table DB
    """
    with tempfile.NamedTemporaryFile(suffix=".db") as file:
        yield f"sqlite:///{file.name}"


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
    engine = create_engine(postgres_uri)
    conn = engine.connect()
    conn.execution_options(stream_results=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


@pytest.fixture(scope="function")
def postgres_source(postgres_db, postgres_conn, postgres_uri):
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """
    data = {"d": [1, 2, 3], "e": [4, 5, 6], "f": [7, 8, None]}
    pd.DataFrame(data=data).to_sql(name="test", con=postgres_conn, index=False)
    yield postgres_uri


@pytest.fixture(scope="function")
def postgres_sink(postgres_db, postgres_uri):
    """
    Yield a SQL Sink with a connection string to an existing Table DB
    """
    yield postgres_uri


@pytest.fixture(scope="function")
def mysql_uri():
    yield "mysql+pymysql://user:pass@mysql:3306/db?charset=utf8mb4"


@pytest.fixture(scope="function")
def mysql_db(mysql_uri):
    if database_exists(mysql_uri):
        drop_database(mysql_uri)
    create_database(mysql_uri)
    yield
    drop_database(mysql_uri)


@pytest.fixture(scope="function")
def mysql_conn(mysql_db, mysql_uri):
    engine = create_engine(mysql_uri)
    conn = engine.connect()
    conn.execution_options(stream_results=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


@pytest.fixture(scope="function")
def mysql_source(mysql_db, mysql_conn, mysql_uri):
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """
    data = {"c": [1, 2, 3], "d": [4, 5, 5], "e": [7, 8, None]}
    pd.DataFrame(data=data).to_sql(name="test", con=mysql_conn, index=False)
    yield mysql_uri


@pytest.fixture(scope="function")
def mysql_sink(mysql_db, mysql_uri):
    """
    Yield a SQL Sink with a connection string to an existing Table DB
    """
    yield mysql_uri


@pytest.fixture(scope="function")
def oracle_uri():
    yield "oracle+cx_oracle://user:pass@oracle:3306?service_name=db"


@pytest.fixture(scope="function")
def oracle_db(oracle_uri):
    if database_exists(oracle_uri):
        drop_database(oracle_uri)
    create_database(oracle_uri)
    yield
    drop_database(oracle_uri)


@pytest.fixture(scope="function")
def oracle_conn(oracle_db, oracle_uri):
    engine = create_engine(oracle_uri)
    conn = engine.connect()
    conn.execution_options(stream_results=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
        engine.dispose()


@pytest.fixture(scope="function")
def oracle_source(oracle_db, oracle_conn, oracle_uri):
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """
    data = {"c": [1, 2, 3], "d": [4, 5, 5], "e": [7, 8, None]}
    pd.DataFrame(data=data).to_sql(name="test", con=oracle_conn, index=False)
    yield oracle_uri


@pytest.fixture(scope="function")
def oracle_sink(oracle_db, oracle_uri):
    """
    Yield a SQL Sink with a connection string to an existing Table DB
    """
    yield oracle_uri
