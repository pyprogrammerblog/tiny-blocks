import logging
import pandas as pd
from typing import Iterator
from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.extract.base import ExtractBase

__all__ = ["FanIn"]


logger = logging.getLogger(__name__)


class Pipe:
    """
    Represent the glue between all operations in an ETL Operation
    """

    def __init__(self, *generator: Iterator[pd.DataFrame]):
        self.generator = generator

    def __rshift__(self, next: TransformBase | LoadBase) -> "Pipe":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            generator = next.get_iter(*self.generator)
            return Pipe(generator)
        elif isinstance(next, LoadBase):
            next.exhaust(*self.generator)
        else:
            raise ValueError("Something")


class FanIn:
    def __init__(self, *args: ExtractBase | Pipe):
        self.generators = []
        for arg in args:
            if isinstance(arg, ExtractBase):
                self.generators.append(arg.get_iter())
            elif isinstance(arg, Pipe):
                self.generators.append(*arg.generator)

    def __rshift__(self, next: TransformBase | LoadBase):
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            generator = next.get_iter(*self.generators)
            return Pipe(generator)
        elif isinstance(next, LoadBase):
            next.exhaust(*self.generators)
        else:
            raise ValueError("Wrong block type")
