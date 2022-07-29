import pandas as pd

from blocks.etl.extract.read_csv import ReadCsvBlock
from blocks.etl.extract.read_sql import ReadSQLBlock
from blocks.etl.transform.merge import MergeBlock
from blocks.etl.transform.fillna import FillnaBlock
from blocks.etl.transform.insert import InsertFromAPIBlock
from blocks.etl.load.to_sql import WriteSQLBlock
from blocks.etl.load.to_csv import WriteCSVBlock


def test_basic_flow(source_sql, source_csv, tempdir):
    """
    Test a basic ETL pipeline
    """

    # extract from two sources
    read_from_csv = ReadCsvBlock()
    read_from_sql = ReadSQLBlock()

    # transform
    merge = MergeBlock()
    fill_na = FillnaBlock()
    enrich = InsertFromAPIBlock()

    # load into a csv
    write_to_csv = WriteCSVBlock()

    # Pipeline
    [read_from_csv, read_from_sql] >> merge >> fill_na >> enrich >> write_to_csv

    # result is at
    assert write_to_csv.sink.path.exists()
    result = pd.read_csv(write_to_csv.sink.path)
    assert result.columns.to_list == []
    assert result.shape == (1, 0)


def test_basic_flow_different_way_of_writting(source_sql, source_csv, tempdir):
    """
    Test a basic ETL pipeline
    """

    # extract from two sources
    read_from_csv = ReadCsvBlock()
    read_from_sql = ReadSQLBlock()

    # transform
    merge = MergeBlock()
    fill_na = FillnaBlock()
    enrich = InsertFromAPIBlock()

    # load into a csv
    write_to_csv = WriteCSVBlock()

    # Pipeline
    gen_1 = read_from_csv.get_iter()
    gen_2 = read_from_sql.get_iter()
    gen = merge.get_iter(gen_1, gen_2)
    gen = fill_na.get_iter(gen_1, gen_2)
    gen = enrich.get_iter(gen_1, gen_2)
    write_to_csv.process(gen)

    # result is at
    assert write_to_csv.sink.path.exists()
    result = pd.read_csv(write_to_csv.sink.path)
    assert result.columns.to_list == []
    assert result.shape == (1, 0)
