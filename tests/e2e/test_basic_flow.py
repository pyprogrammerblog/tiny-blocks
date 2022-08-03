import pandas as pd
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.extract.from_csv import ExtractCSV
from tiny_blocks.load.to_csv import LoadCSV
from tiny_blocks.transform.fillna import Fillna
from tiny_blocks.transform.drop_duplicates import DropDuplicates
from tiny_blocks.transform.merge import Merge
from tiny_blocks.pipeline import FanIn
import tempfile


def test_basic_flow(csv_source, postgres_source):
    """
    Test a basic ETL pipeline
    """

    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        # 1. Extract from two sources
        csv = ExtractCSV(path=csv_source)
        postgres = ExtractSQLTable(dsn_conn=postgres_source, table_name="test")

        # 2. Transform
        merge = Merge(how="left", left_on="c", right_on="d")
        fill_na = Fillna(value="Hola Mundo")
        drop_duplicates = DropDuplicates()

        # 3. Load
        to_csv = LoadCSV(path=file.name)

        ###########
        # Pipeline
        FanIn(csv, postgres) >> merge >> fill_na >> drop_duplicates >> to_csv

        # testing
        assert to_csv.path.exists()
        df = pd.read_csv(to_csv.path, sep="|")
        assert df.shape == (3, 6)
        assert not df.isnull().values.any()
