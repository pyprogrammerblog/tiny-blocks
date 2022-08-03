import pandas as pd
from tiny_blocks.extract.from_csv import ExtractCSV
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.load.to_csv import LoadCSV
from tiny_blocks.load.to_sql import LoadSQL
from tiny_blocks.transform.fillna import Fillna
from tiny_blocks.transform.merge import Merge
from tiny_blocks.pipeline import FanIn, FanOut
import tempfile


def test_basic_flow(sqlite_source, csv_source, sqlite_sink):
    """
    Test a basic ETL pipeline
    """

    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        # 1. Extract from two sources
        read_from_csv = ExtractCSV(path=csv_source)
        read_from_sql = ExtractSQLTable(dsn_conn=sqlite_source)

        # 2. Transform
        merge = Merge()
        fill_na = Fillna()

        # 3. Load
        write_to_csv = LoadCSV(path=file.name)
        write_to_sql = LoadSQL(dsn_conn=sqlite_sink)

        ###########
        # Pipeline
        FanIn(read_from_csv, read_from_sql) >> merge >> fill_na >> FanOut(
            write_to_csv, write_to_sql
        )

        # testing
        assert write_to_csv.path.exists()
        result = pd.read_csv(write_to_csv.path)
        assert result.columns.to_list == []
        assert result.shape == (1, 0)
