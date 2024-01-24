# from pydantic import Field, BaseModel
# from typing import Iterator, Literal, Dict
# from tiny_blocks.transform.base import TransformBase
# from sqlmodel import Session, SQLModel, create_engine, select, text
#
# import itertools
# import logging
# import tempfile
#
#
# __all__ = ["Sort"]
#
#
# logger = logging.getLogger(__name__)
#
#
# class Sort(TransformBase):
#     """
#     Sort Block. Defines the Sorting operation
#
#     Basic example:
#         >>> from tiny_blocks.transform import Sort
#         >>> from tiny_blocks.extract import FromCSV
#         >>>
#         >>> extract_csv = FromCSV(path='/path/to/file.csv')
#         >>> sort = Sort(by=["column_A"], ascending=False)
#         >>>
#         >>> generator = extract_csv.get_iter()
#         >>> generator = sort.get_iter(generator)
#     """
#
#     name: Literal["sort"] = Field(default="sort")
#     by: Dict[str, Literal["asc", "desc"]] = Field(description="Sorted by")
#
#     def get_iter(self, source: Iterator[BaseModel]):
#
#         with tempfile.NamedTemporaryFile(suffix=".sqlite") as file:
#
#             # use first row for extracting fieldnames
#             try:
#                 first_row = next(source)
#             except StopIteration:
#                 raise ValueError(f"Source is empty. No data to write.")
#
#             # create table
#             class ToBeSortTable(first_row.__class__, SQLModel, table=True):
#                 pass
#
#             class SortedTable(first_row.__class__, SQLModel, table=True):
#                 pass
#
#             engine = create_engine(file.name, echo=True)
#             SQLModel.metadata.create_all(engine)
#
#             # write records in sql
#             with Session(engine) as session:
#                 for row in itertools.chain([first_row], source):
#                     session.add(ToBeSortTable(**row.dict()))
#                 session.commit()
#
#                 # Sort by statement
#                 group_by = ", ".join([f"{k} {v}" for k, v
#                 in self.by.items()])
#                 statement = select(ToBeSortTable).group_by(text(group_by))
#
#                 # yield iterator
#                 for row in session.exec(statement).fetchmany(size=1000):
#                     session.add(SortedTable(**row.dict()))
#                 session.commit()
#
#
# # download >> read >> sort >> add_timestamp >>  write >> delete
