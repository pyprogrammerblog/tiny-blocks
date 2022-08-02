import pandas as pd
from tiny_blocks.extract.from_csv import ExtractCSV
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.load.to_csv import LoadCSV
from tiny_blocks.transform.fillna import Fillna
from tiny_blocks.transform.merge import MergeBlock


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
    assert write_to_csv.path.exists()
    result = pd.read_csv(write_to_csv.path)
    assert result.columns.to_list == []
    assert result.shape == (1, 0)
