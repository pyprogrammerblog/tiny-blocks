# import logging
# import itertools
# from pydantic import Field, BaseModel, create_model
# from typing import Iterator, Literal, Any, Set, Type
# from tiny_blocks.transform.base import TransformBase
#
#
# __all__ = ["FillNone"]
#
#
# logger = logging.getLogger(__name__)
#
#
# class FillNone(TransformBase):
#     """
#     Fill None Block. Defines the fill Nan values functionality
#
#     Basic example:
#         >>> from tiny_blocks.transform import FillNone
#         >>> from tiny_blocks.extract import FromCSV
#         >>>
#         >>> extract_csv = FromCSV(path='/path/to/file.csv')
#         >>> fill_na = FillNone(value="Hola Mundo")
#         >>>
#         >>> generator = extract_csv.get_iter()
#         >>> generator = fill_na.get_iter(generator)
#     """
#
#     name: Literal["fill_none"] = "fill_none"
#     value: Any = Field(description="Value to be filled")
#     subset: Set[str] = Field(description=
#     "Subset to apply this default value")
#
#     def get_iter(self, source: Iterator[BaseModel]) -> Iterator[BaseModel]:
#
#         # extract first row to retrieve the model
#         first_row = next(source)
#         input_model = first_row.__class__
#
#         # create an output model and yield the rows
#         output_model = self._output_model(input_model=input_model)
#         for row in itertools.chain([first_row], source):
#             yield output_model(**row.dict(exclude_none=True))
#
#     def _output_model(self, input_model: Type[BaseModel]) -> Type[BaseModel]:
#
#         if missing := set(input_model.__fields__.keys()) - self.subset:
#             raise ValueError(f"Fields {','.join(missing)} not found.")
#
#         output_model = create_model(
#             __base__=input_model,
#             __model_name=f"Output_{self.name}_{self.uuid}",
#             **{field_name: self.value for field_name in self.subset},
#         )
#
#         return output_model
