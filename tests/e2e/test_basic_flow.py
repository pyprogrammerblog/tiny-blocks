# import pandas as pd
# from tiny_blocks import FanIn, FanOut
# from tiny_blocks.extract import FromSQL
# from tiny_blocks.extract import FromCSV
# from tiny_blocks.load import ToCSV
# from tiny_blocks.load import ToSQL
# from tiny_blocks.transform import FillNone
# from tiny_blocks.transform import Apply
# from tiny_blocks.transform import Rename
# from tiny_blocks.transform import DropDuplicates
# from tiny_blocks.transform import Merge
#
#
# def test_basic_flow(csv_source, postgres_source, csv_sink):
#     """
#     Test a basic ETL pipeline
#     """
#     # 1. Extract from two sources
#     csv = FromCSV(path=csv_source)
#
#     # 2. Transform
#     fill_na = FillNone(value="Hola Mundo")
#     drop_dupl = DropDuplicates()
#
#     # 3. Load
#     to_csv = ToCSV(path=csv_sink)
#
#     ###########
#     # Pipeline
#     csv >> fill_na >> drop_dupl >> to_csv
#
#     # testing
#     assert to_csv.path.exists()
#     df = pd.read_csv(to_csv.path, sep="|")
#     assert not df.empty
#     assert df.shape == (3, 3)
#     assert not df.isnull().values.any()
#
#
# def test_basic_flow_fan_in(csv_source, postgres_source, csv_sink):
#     """
#     Test a basic ETL pipeline
#     """
#     # 1. Extract from two sources
#     csv = FromCSV(path=csv_source)
#     postgres = FromSQL(dsn_conn=postgres_source, table_name="test")
#
#     # 2. Transform
#     merge = Merge(how="left", left_on="c", right_on="d")
#     fill_na = FillNone(value="Hola Mundo")
#     drop_dupl = DropDuplicates()
#
#     # 3. Load
#     to_csv = ToCSV(path=csv_sink)
#
#     ###########
#     # Pipeline
#     FanIn(csv, postgres) >> merge >> fill_na >> drop_dupl >> to_csv
#
#     # testing
#     assert to_csv.path.exists()
#     df = pd.read_csv(to_csv.path, sep="|")
#     assert not df.empty
#     assert df.shape == (3, 6)
#     assert not df.isnull().values.any()
#
#
# def test_basic_flow_fan_out(sqlite_source, csv_sink, postgres_sink):
#     """
#     Test a basic ETL pipeline
#     """
#     # 1. Extract from two sources
#     from_sql = FromSQL(dsn_conn=sqlite_source, table_name="TEST")
#
#     # 2. Transform
#     fill_na = FillNone(value="Hola Mundo")
#     rename = Rename(columns={"f": "F"})
#
#     # 3. Load
#     to_csv = ToCSV(path=csv_sink)
#     to_postgres = ToSQL(dsn_conn=postgres_sink, table_name="test")
#
#     ###########
#     # Pipeline
#     from_sql >> FanOut(to_csv) >> fill_na >> rename >> to_postgres
#
#     # testing
#     assert to_csv.path.exists()
#     df = pd.read_csv(to_csv.path, sep="|")
#     assert not df.empty
#     assert df.shape == (3, 3)
#     assert df.columns.to_list() == ["d", "e", "f"]
#     assert df.isnull().values.any()
#
#     df = pd.read_sql_table(table_name="test", con=postgres_sink)
#     assert df.shape == (3, 3)
#     assert df.columns.to_list() == ["d", "e", "F"]
#     assert not df.isnull().values.any()
#     assert "Hola Mundo" in df.F.values
#
#
# def test_complex_flows_with_apply(
#     csv_source, csv_sink, csv_sink_2, csv_sink_3
# ):
#
#     from_csv = FromCSV(path=csv_source)
#
#     to_csv_1 = ToCSV(path=csv_sink)
#     to_csv_2 = ToCSV(path=csv_sink_2)
#     to_csv_3 = ToCSV(path=csv_sink_3)
#
#     apply = Apply(apply_to_column="a",
#     set_to_column="a", func=lambda x: x + 1)
#
#     """
#     csv ->|-> apply -> apply -> |-> csv3 (+2)
#           |-> csv1 (+0)         |-> csv2 (+2)
#     """
#     (
#         from_csv
#         >> FanOut(to_csv_1)
#         >> apply
#         >> apply
#         >> FanOut(to_csv_2, to_csv_3)
#     )
#
#     assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 1
#     assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 3
#     assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3
#
#     """
#     csv ->|-> apply -> apply -> csv3 (+2)
#           |-> csv2 (+0)
#           |-> csv1 (+0)
#     """
#     from_csv >> FanOut(to_csv_1, to_csv_2) >> apply >> apply >> to_csv_3
#
#     assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 1
#     assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 1
#     assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3
#
#     (
#         from_csv
#         >> apply
#         >> apply
#         >> FanOut(to_csv_1)  # csv1 2
#         >> apply
#         >> apply
#         >> FanOut(to_csv_2, to_csv_3)  # csv2,csv3 3
#     )
#
#     assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 3
#     assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 5
#     assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 5
#
#
# def test():
#
#     assert
#
#     pipeline()
#
#     assert
