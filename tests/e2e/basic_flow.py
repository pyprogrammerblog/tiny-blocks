import pandas as pd

from tiny_blocks.etl.extract.from_csv import ExtractCSV
from tiny_blocks.etl.extract.from_sql_table import ReadSQLTableBlock
from tiny_blocks.etl.transform.merge import MergeBlock
from tiny_blocks.etl.transform.fillna import FillnaBlock
from tiny_blocks.etl.transform.enrich_from_api import InsertFromAPIBlock
from tiny_blocks.etl.load.to_csv import LoadCSVBlock


def test_basic_flow(sql_source, csv_source, tempdir):
    """
    Test a basic ETL pipeline
    """

    # 1. Extract from two sources
    read_from_csv = ExtractCSV()
    read_from_sql = ReadSQLTableBlock()

    # 2. Transform
    merge = MergeBlock()
    fill_na = FillnaBlock()
    enrich = InsertFromAPIBlock()

    # 3. Load
    write_to_csv = LoadCSVBlock()

    ###########
    # Pipeline
    sources = [read_from_csv, read_from_sql]
    sources >> merge >> fill_na >> enrich >> write_to_csv

    # testing
    assert write_to_csv.sink.path.exists()
    result = pd.read_csv(write_to_csv.sink.path)
    assert result.columns.to_list == []
    assert result.shape == (1, 0)


def test_basic_flow_different_way_of_writting(sql_source, csv_source, tempdir):
    """
    Test a basic ETL pipeline
    """

    # extract from two sources
    read_from_csv = ExtractCSV()
    read_from_sql = ReadSQLTableBlock()

    # transform
    merge = MergeBlock()
    fill_na = FillnaBlock()
    enrich = InsertFromAPIBlock()

    # load into a csv
    write_to_csv = LoadCSVBlock()

    # Pipeline
    generator_1 = read_from_csv.get_iter()
    generator_2 = read_from_sql.get_iter()
    generator = merge.get_iter(generator_1, generator_2)
    generator = fill_na.get_iter(generator)
    generator = enrich.get_iter(generator)
    write_to_csv.exhaust(generator=generator)

    # result is at
    assert write_to_csv.sink.path.exists()
    result = pd.read_csv(write_to_csv.sink.path)
    assert result.columns.to_list == []
    assert result.shape == (1, 0)
