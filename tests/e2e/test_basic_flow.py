import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable
from tiny_blocks.extract.from_csv import FromCSV
from tiny_blocks.load.to_csv import ToCSV
from tiny_blocks.transform.fillna import Fillna
from tiny_blocks.transform.drop_duplicates import DropDuplicates
from tiny_blocks.transform.merge import Merge
from tiny_blocks.pipeline import FanIn, Pipeline


def test_basic_flow(csv_source, postgres_source, csv_sink):
    """
    Test a basic ETL pipeline
    """
    # 1. Extract from two sources
    csv = FromCSV(path=csv_source)

    # 2. Transform
    fill_na = Fillna(value="Hola Mundo")
    drop_dupl = DropDuplicates()

    # 3. Load
    to_csv = ToCSV(path=csv_sink)

    ###########
    # Pipeline
    with Pipeline(name="Pipeline 1"):
        csv >> fill_na >> drop_dupl >> to_csv

    # testing
    assert to_csv.path.exists()
    df = pd.read_csv(to_csv.path, sep="|")
    assert not df.empty
    assert df.shape == (3, 3)
    assert not df.isnull().values.any()


def test_basic_flow_fan_in(csv_source, postgres_source, csv_sink):
    """
    Test a basic ETL pipeline
    """
    # 1. Extract from two sources
    csv = FromCSV(path=csv_source)
    postgres = FromSQLTable(dsn_conn=postgres_source, table_name="test")

    # 2. Transform
    merge = Merge(how="left", left_on="c", right_on="d")
    fill_na = Fillna(value="Hola Mundo")
    drop_dupl = DropDuplicates()

    # 3. Load
    to_csv = ToCSV(path=csv_sink)

    ###########
    # Pipeline
    FanIn(csv, postgres) >> merge >> fill_na >> drop_dupl >> to_csv

    # testing
    assert to_csv.path.exists()
    df = pd.read_csv(to_csv.path, sep="|")
    assert not df.empty
    assert df.shape == (3, 6)
    assert not df.isnull().values.any()
