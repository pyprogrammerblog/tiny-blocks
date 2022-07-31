import pytest
import pandas as pd
from sqlite3 import connect
from tempfile import TemporaryFile
from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.sources.sql import SQLSource
from tiny_blocks.sinks.csv import CSVSink
from tiny_blocks.sinks.sql import SQLSink


@pytest.fixture
def csv_source():
    """
    Yield a CSV Source with a path to an existing CSV file
    """
    with TemporaryFile(suffix=".csv") as file:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
        pd.DataFrame(data=data).to_csv(file, sep="|")
        yield CSVSource(path=file)


@pytest.fixture
def csv_sink():
    """
    Yield a CSV Sink with a path to an existing CSV file
    """
    with TemporaryFile(suffix=".csv") as file:
        yield CSVSink(path=file)


@pytest.fixture
def sql_source():
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """
    with TemporaryFile(suffix=".sqlite") as file, connect(file) as con:
        data = {"c": [1, 2, 3], "d": [4, 5, 6], "e": [7, 8, 9]}
        pd.DataFrame(data=data).to_sql(name="TEST", con=con)
        yield SQLSource(conn=con)


@pytest.fixture
def sql_sink():
    """
    Yield a SQL Sink with a connection string to an existing Table DB
    """
    with TemporaryFile(suffix=".sqlite") as file, connect(file) as con:
        yield SQLSink(conn=con)
