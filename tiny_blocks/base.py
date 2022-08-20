from uuid import UUID, uuid4
from pydantic import BaseModel, Field
import itertools
import logging
from typing import List, Iterator, Union, NoReturn

import pandas as pd

from tiny_blocks.extract.base import ExtractBase
from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase

logger = logging.getLogger(__name__)


__all__ = ["FanIn", "FanOut"]


class KwargsBase(BaseModel):
    """
    Base for Kwargs Models
    """

    def to_dict(self):
        return self.dict(exclude_none=True)

    def to_json(self):
        return self.json(exclude_none=True)


class BaseBlock(BaseModel):
    """
    Base Block class
    """

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(..., description="Block name")
    description: str = Field(default=None, description="Description")

    def __str__(self):
        return f"Block-{self.name}-{self.uuid}"


class Pipe:
    def __init__(self, source: Iterator[pd.DataFrame]):
        self.source = source

    def __rshift__(
        self, next: Union[TransformBase, LoadBase]
    ) -> Union["Pipe", NoReturn]:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.source)
            return Pipe(source=source)
        elif isinstance(next, LoadBase):
            return next.exhaust(source=self.source)
        elif isinstance(next, FanOut):
            # n sources = a source per each load block + 1 for the next pipe
            n = len(next.load_blocks) + 1
            source, *sources = itertools.tee(self.source, n)
            next.exhaust(sources)
            return Pipe(source=source)
        else:
            raise ValueError("Unsupported Block Type")

    def get_iter(self):
        return self.source


class FanIn:
    """
    Gather multiple operations and send them to the next block.
    The next block must accept multiple arguments, like for example:
    ``tiny_blocks.tranform.Merge``

    For now, FanIn can just gather Extraction Blocks.

    Usage:
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToSQL
        >>> from tiny_blocks import FanIn, Pipeline
        >>> from tiny_blocks.transform import Merge
        >>>
        >>> csv_1 = FromCSV(path='/path/to/file1.csv')
        >>> csv_2 = FromCSV(path='/path/to/file2.csv')
        >>> merge = Merge(left_on="ColumnA", right_on="ColumnB")
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postgres://...')
        >>>
        >>> FanIn(csv_1, csv_2)  >> merge >> to_sql
    """

    def __init__(self, *pipes: Union[ExtractBase, "Pipe"]):
        self.pipes = pipes

    def __rshift__(self, next: TransformBase) -> "Pipe":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.get_iter())
            return Pipe(source=source)
        else:
            raise ValueError("Unsupported Block Type")

    def get_iter(self) -> List[Iterator[pd.DataFrame]]:
        return [pipe.get_iter() for pipe in self.pipes]


class FanOut:
    """
    Tee the flow into multiple pipes.

    Usage:
        >>> from tiny_blocks import FanOut
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToSQL, ToCSV
        >>> from tiny_blocks.transform import DropDuplicates
        >>>
        >>> from_csv = FromCSV(path='/path/to/source.csv')
        >>> drop_duplicates = DropDuplicates()
        >>> to_csv = ToCSV(path='/path/to/sink.csv')
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postgres://...')
        >>>
        >>> from_csv >> FanOut(to_sql) >> drop_duplicates >> to_csv
    """

    def __init__(self, *load_blocks: LoadBase):
        self.load_blocks = load_blocks

    def exhaust(self, *sources: Iterator[pd.DataFrame]):
        for load_block, source in zip(self.load_blocks, sources):
            try:
                load_block.exhaust(source=source)
            except Exception as e:
                logger.error(str(e))
