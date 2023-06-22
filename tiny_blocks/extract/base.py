import logging
import itertools

from pydantic import (
    BaseModel,
    create_model_from_typeddict,
    create_model_from_namedtuple,
)
from dataclasses import dataclass, is_dataclass
from typing import Iterator, NoReturn, Union, TypedDict, NamedTuple
from tiny_blocks.base import BaseBlock
from tiny_blocks.load.base import LoadBase
from tiny_blocks.utils import Pipeline, FanOut, create_model_from_dataclass
from tiny_blocks.transform.base import TransformBase


__all__ = ["ExtractBase"]


logger = logging.getLogger(__name__)


class ExtractBase(BaseBlock):
    """
    Extract Base Block.

    Each extraction Block implements the `get_iter` method.
    This method returns an Iterator of chunked DataFrames
    """

    def __int__(
        self,
        row_model: Union[dataclass | BaseModel | TypedDict | NamedTuple],
        lazy_validation: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.lazy_validation = lazy_validation

        if issubclass(row_model, BaseModel):
            self.row_model = row_model
        elif is_dataclass(row_model):
            self.row_model = create_model_from_dataclass(row_model)
        elif issubclass(row_model, TypedDict):
            self.row_model = create_model_from_typeddict(row_model)
        elif issubclass(row_model, NamedTuple):
            self.row_model = create_model_from_namedtuple(row_model)
        else:
            raise TypeError("The row model provided is not a defined type.")

    def get_iter(self) -> Iterator[dataclass]:
        """
        Return an iterator of dataclasses
        """
        raise NotImplementedError

    def __rshift__(
        self, next: TransformBase | LoadBase | FanOut
    ) -> NoReturn | Pipeline:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.get_iter())
            return Pipeline(source)
        elif isinstance(next, LoadBase):
            return next.exhaust(source=self.get_iter())
        elif isinstance(next, FanOut):
            # n sources = a source per each load block + 1 for the next pipe
            n = len(next.sinks) + 1
            source, *sources = itertools.tee(self.get_iter(), n)
            next.exhaust(*sources)
            return Pipeline(source=source)
        else:
            raise ValueError("Unsupported Block Type")
