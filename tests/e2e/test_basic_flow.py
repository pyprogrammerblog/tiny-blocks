from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.load.to_csv import LoadCSV
from tiny_blocks.transform.fillna import Fillna
from tiny_blocks.transform.merge import Merge
from tiny_blocks.pipeline import FanIn
import tempfile


def test_basic_flow(postgres_source, mysql_source, sqlite_sink):
    """
    Test a basic ETL pipeline
    """

    with tempfile.NamedTemporaryFile(suffix=".csv") as file:
        # 1. Extract from two sources
        postgres = ExtractSQLTable(dsn_conn=postgres_source, table_name="test")
        mysql = ExtractSQLTable(dsn_conn=mysql_source, table_name="test")

        # 2. Transform
        merge = Merge(how="left", left_on="c", right_on="c")
        fill_na = Fillna(value="Hola Mundo")

        # 3. Load
        to_csv = LoadCSV(path=file.name)

        ###########
        # Pipeline
        FanIn(postgres, mysql) >> merge >> fill_na >> to_csv

        # testing
        assert to_csv.path.exists()
        df = pd.read_csv(to_csv.path, sep="|")
        assert df.shape == (3, 6)
        assert not df.isnull().values.any()
