import itertools
import logging
from typing import List, Iterator, Union, NoReturn
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.load.base import LoadBase

import pandas as pd
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tiny_blocks.extract.base import ExtractBase


__all__ = ["FanIn", "FanOut", "Pipeline"]


logger = logging.getLogger(__name__)


class FanOut:
    """
    Tee the flow into one/multiple pipes.
    The main pipeline can continue as shown in the example.

    Usage::

        ... >> FanOut(load1, load2, ..., loadN) >> ...


    Examples:
        >>> from tiny_blocks import FanOut
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToSQL, ToCSV
        >>> from tiny_blocks.transform import DropDuplicates, FillNone
        >>>
        >>> from_csv = FromCSV(path='/path/to/source.csv')
        >>> drop_dupl = DropDuplicates()
        >>> fill_na = FillNone(value="Hola Mundo")
        >>> to_csv = ToCSV(path='/path/to/sink.csv')
        >>> to_sql = ToSQL(dsn_conn='psycopg2+po...', table_name="sink")
        >>>
        >>> from_csv >> FanOut(to_sql) >> drop_dupl >> to_csv
    """

    def __init__(self, *sinks: LoadBase):
        self.sinks = sinks

    def exhaust(self, *sources: Iterator[pd.DataFrame]):
        for sink, source in zip(self.sinks, sources):
            try:
                sink.exhaust(source)
            except Exception as e:
                logger.error(str(e))


class Pipeline:
    """
    Defines the glue between all blocks.

    It gets created by a FanIn or ExtractBlock and from there
    it joins all blocks till there is a sink.
    """

    def __init__(self, source: Iterator[pd.DataFrame]):
        self.source = source

    def get_iter(self):
        return self.source

    def __rshift__(
        self, next: TransformBase | LoadBase | FanOut
    ) -> "Pipeline" | NoReturn:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.get_iter())
            return Pipe(source)
        elif isinstance(next, LoadBase):
            return next.exhaust(source=self.get_iter())
        elif isinstance(next, FanOut):
            # a source per each load block + 1 for the next pipe
            n = len(next.sinks) + 1
            source, *sources = itertools.tee(self.get_iter(), n)
            next.exhaust(*sources)
            return Pipeline(source=source)
        else:
            raise ValueError("Unsupported Block Type")


class FanIn:
    """
    Gather multiple operations and send them to the next block.
    The next block must accept multiple arguments, for example:
    ``tiny_blocks.tranform.Merge``

    Usage::

        FanIn(pipe1, pipe2, ..., pipeN) >> ...


    Examples:
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToCSV
        >>> from tiny_blocks.utils import FanIn
        >>> from tiny_blocks.transform import Merge, FillNone
        >>>
        >>> from_csv_1 = FromCSV(path='/path/to/file1.csv')
        >>> from_csv_2 = FromCSV(path='/path/to/file2.csv')
        >>> to_csv = ToCSV(path='/path/to/file3.csv')
        >>> fill_none = FillNone(value="Hola Mundo")
        >>> merge = Merge(left_on="ColumnA", right_on="ColumnB")
        >>>
        >>> FanIn(from_csv_1, from_csv_2 >> fill_none)  >> merge >> to_csv
    """

    def __init__(self, *pipes: Union["ExtractBase", "Pipeline"]):
        self.pipes = pipes

    def __rshift__(self, next: TransformBase) -> "Pipeline":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.get_iter())
            return Pipeline(source=source)
        else:
            raise ValueError("Unsupported Block Type")

    def get_iter(self) -> List[Iterator[pd.DataFrame]]:
        return [pipe.get_iter() for pipe in self.pipes]
