import logging
from tiny_blocks.base import Row
from typing import Literal, Iterator, Type
from pydantic import Field, BaseModel, ValidationError
from tiny_blocks.transform.base import TransformBase


__all__ = ["Validate"]


logger = logging.getLogger(__name__)


class Validate(TransformBase):
    """
    Validate block.
    """

    name: Literal["Validate"] = Field(default="validate")
    model: Type[BaseModel] = Field(..., description="Validation Model")

    def get_iter(self, source: Iterator[Row]) -> Iterator[Row]:

        errors = []  # collector of validation failures

        for row in source:
            try:
                yield self.model.validate(row).dict()
            except ValidationError as errs:
                errors.append(errs.errors())

        if errors:
            raise ValidationError(model=self.model, errors=errors)
