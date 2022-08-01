import pandas as pd

from tiny_blocks.etl.extract.from_csv import ExtractCSV
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.etl.transform.merge import MergeBlock
from tiny_blocks.etl.transform.fillna import Fillna
from tiny_blocks.etl.load.to_csv import LoadCSV


def test_basic_flow(sqlite_source, csv_source):
    """
    Test a basic ETL pipeline
    """

    # 1. Extract from two sources
    read_from_csv = ExtractCSV()
    read_from_sql = ExtractSQLTable()

    # 2. Transform
    merge = MergeBlock()
    fill_na = Fillna()

    # 3. Load
    write_to_csv = LoadCSV()

    ###########
    # Pipeline
    sources = [read_from_csv, read_from_sql]
    sources >> merge >> fill_na >> write_to_csv

    # testing
    assert write_to_csv.sink.path.exists()
    result = pd.read_csv(write_to_csv.sink.path)
    assert result.columns.to_list == []
    assert result.shape == (1, 0)
