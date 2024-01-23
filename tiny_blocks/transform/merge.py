# import logging
# import tempfile
# from sqlite3 import connect
# from pydantic import Field, BaseModel
# from typing import Iterator, Literal, List
# from tiny_blocks.transform.base import TransformBase
#
#
# __all__ = ["Merge"]
#
#
# logger = logging.getLogger(__name__)
#
#
# class Merge(TransformBase):
#     """
#     Merge. Defines merge functionality between two blocks.
#
#     Basic example:
#         >>> from tiny_blocks.transform import Merge
#         >>> from tiny_blocks.extract import FromCSV
#         >>>
#         >>> from_csv_1 = FromCSV(path="/path/to/file_1.csv")
#         >>> from_csv_2 = FromCSV(path="/path/to/file_2.csv")
#         >>> merge = Merge(how="left", left_on="col_A", right_on="col_B")
#         >>>
#         >>> left_source = from_csv_1.get_iter()
#         >>> right_source = from_csv_2.get_iter()
#         >>> generator = merge.get_iter(source=[left_source, right_source])
#     """
#
#     name: Literal["merge"] = Field(default="merge")
#     how: Literal["left", "right", "outer", "inner", "cross"] = "inner"
#     left_on: str = Field(..., description="Column on the left table")
#     right_on: str = Field(..., description="Column on the right table")
#
#     def get_iter(
#         self, source: List[Iterator[BaseModel]]
#     ) -> Iterator[BaseModel]:
#
#         with tempfile.NamedTemporaryFile(suffix=".sqlite") as file, connect(
#             file.name
#         ) as con:
#             left_source, right_source = source
#
#             # send records to a temp database (exhaust the generators)
#             for chunk in left_source:
#                 chunk.to_sql(name="table_left", con=con, index=False)
#
#             for chunk in right_source:
#                 chunk.to_sql(name="table_right", con=con, index=False)
#
#             # select non-duplicated rows.
#             # It is possible to select a non-duplicated subset of rows.
#             sql = (
#                 f"SELECT * FROM table_left "
#                 f"{self.how.capitalize()} JOIN table_right "
#                 f"ON table_left.{self.left_on}"
#                 f" = table_right.{self.right_on}"
#             )
#
#             # yield joined records
#             kwargs = self.kwargs.dict()
#             for chunk in con.execute(sql=sql):
#                 yield chunk
