import pytest
import pandas as pd
from sqlite3 import connect
from tempfile import TemporaryFile
from blocks.sources.csv import CSVSource
from blocks.sources.sql import SQLSource


@pytest.fixture
def source_csv():
    """
    Yield a CSV Source with a path to an existing CSV file
    """
    with TemporaryFile(suffix=".csv") as file:
        data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
        pd.DataFrame(data=data).to_csv(file, sep="|")
        yield CSVSource(path=file)


@pytest.fixture
def source_sql():
    """
    Yield a SQL Source with a connection string to an existing Table DB
    """
    with TemporaryFile(suffix=".sqlite") as file, connect(file) as con:
        data = {"c": [1, 2, 3], "d": [4, 5, 6], "e": [7, 8, 9]}
        pd.DataFrame(data=data).to_sql(name="TEST", con=con)
        yield SQLSource(conn=con)
