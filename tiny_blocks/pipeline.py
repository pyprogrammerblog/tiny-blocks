import functools
import sys
from typing import Callable
from datetime import datetime
import itertools
import logging
from typing import List, Iterator, Union, NoReturn
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.load.base import LoadBase

import pandas as pd
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tiny_blocks.extract.base import ExtractBase


__all__ = ["FanIn", "FanOut", "Tee", "Pipeline"]


logger = logging.getLogger(__name__)


class Sink:
    def __init__(self, exhaust: functools.partial):
        self.exhaust = exhaust  # type: ignore

    def exhaust(self, source: Iterator[pd.DataFrame]):
        self.exhaust(source)


class FanOut:
    """
    Tee the flow into one/multiple pipes.
    The main pipeline can continue as shown in the example.

    Usage::

        ... >> FanOut(sink1, sink2, ..., sinkN) >> ...


    Examples:
        >>> from tiny_blocks.pipeline import FanOut
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToSQL, ToCSV
        >>> from tiny_blocks.transform import DropDuplicates, Fillna
        >>>
        >>> from_csv = FromCSV(path='/path/to/source.csv')
        >>> drop_dupl = DropDuplicates()
        >>> fill_na = Fillna(value="Hola Mundo")
        >>> to_csv = ToCSV(path='/path/to/sink.csv')
        >>> to_sql = ToSQL(dsn_conn='psycopg2+po...', table_name="sink")
        >>>
        >>> from_csv >> FanOut(fill_na >> to_sql) >> drop_dupl >> to_csv
    """

    def __init__(self, *sinks: LoadBase | Sink):
        self.sinks = sinks

    def exhaust(self, *sources: Iterator[pd.DataFrame]):
        for sink, source in zip(self.sinks, sources):
            try:
                sink.exhaust(source)
            except Exception as e:
                logger.error(str(e))


class Tee:
    """
    Tee the flow into two or multiple pipes:

    Usage::

        ... >> Tee(sink1, sink2, ..., sinkN)


    Examples:
        >>> from tiny_blocks.pipeline import FanOut
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToSQL, ToCSV
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.transform import DropDuplicates
        >>> from tiny_blocks.transform import Rename
        >>>
        >>> from_csv = FromCSV(path='/path/to/source.csv')
        >>> drop_dupl = DropDuplicates()
        >>> rename = Rename(columns={'a': "A"})
        >>> fillna = Fillna(value="Hola Mundo")
        >>> to_csv = ToCSV(path='/path/to/sink.csv')
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postg...', table_name="sink")
        >>>
        >>> pipe_1 = from_csv >> drop_dupl
        >>> pipe_2 = rename >> to_csv
        >>> pipe_1 >> Tee(pipe_2, to_sql)
    """

    def __init__(self, *sinks: LoadBase | Sink):
        self.sinks = sinks

    def exhaust(self, *sources: Iterator[pd.DataFrame]):
        for sink, source in zip(self.sinks, sources):
            try:
                sink.exhaust(source)
            except Exception as e:
                logger.error(str(e))


class Pipe:
    """
    Defines the glue between all blocks.

    It gets created by a FanIn or ExtractBlock and from there
    it joins all blocks till there is a sink.
    """

    def __init__(self, source: Iterator[pd.DataFrame] | functools.partial):
        self.source = source

    def get_iter(self):
        return self.source

    def __rshift__(
        self, next: TransformBase | LoadBase | Sink | FanOut | Tee
    ) -> "Pipe" | NoReturn:
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            source = next.get_iter(source=self.get_iter())
            return Pipe(source)
        elif isinstance(next, (LoadBase, Sink)):
            return next.exhaust(source=self.get_iter())
        elif isinstance(next, FanOut):
            # n sources = a source per each load block
            # + 1 for the next pipe
            n = len(next.sinks) + 1
            source, *sources = itertools.tee(self.get_iter(), n)
            next.exhaust(*sources)
            return Pipe(source=source)
        elif isinstance(next, Tee):
            # a source per each Sink
            n = len(next.sinks)
            return next.exhaust(*itertools.tee(self.get_iter(), n))
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
        >>> from tiny_blocks.pipeline import FanIn
        >>> from tiny_blocks.transform import Merge
        >>> from tiny_blocks.transform import Fillna
        >>>
        >>> from_csv_1 = FromCSV(path='/path/to/file1.csv')
        >>> from_csv_2 = FromCSV(path='/path/to/file2.csv')
        >>> to_csv = ToCSV(path='/path/to/file3.csv')
        >>> fillna = Fillna(value="Hola Mundo")
        >>> merge = Merge(left_on="ColumnA", right_on="ColumnB")
        >>>
        >>> FanIn(from_csv_1, from_csv_2 >> fillna)  >> merge >> to_csv
    """

    def __init__(self, *pipes: Union["ExtractBase", "Pipe"]):
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


class Pipeline:
    """
    Defines a Pipeline context manager.

    Params:
        - name: (str). Name of the Pipeline
        - description: (str). Description of the Pipeline
        - supress_info: (bool). Supress info about the pipeline result
        - supress_exception: (bool). Supress Pipeline exception if it happens

    Usage:
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.load import ToSQL
        >>> from tiny_blocks.pipeline import Pipeline
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> fill_na = Fillna(value="Hola Mundo")
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postgr...', table_name="sink")
        >>>
        >>> with Pipeline(name="My Pipeline") as pipe:
        >>>     from_csv >> fill_na >> to_sql
    """

    PENDING: str = "PENDING"
    STARTED: str = "STARTED"
    SUCCESS: str = "SUCCESS"
    FAIL: str = "FAIL"

    def __init__(
        self,
        name: str,
        description: str = None,
        supress_output_message: bool = False,
        supress_exception: bool = False,
    ):
        self.name: str = name
        self.description: str | None = description
        self.supress_exception: bool = supress_exception
        self.supress_output_message: bool = supress_output_message
        self.status: str = Pipeline.PENDING
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.detail: str = ""
        self._callables: List = [Callable]

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.status = Pipeline.STARTED
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.utcnow()
        if exc_type:
            self.detail = f"Failure: {exc_val}\n"
            self.status = Pipeline.FAIL
        else:
            self.status = Pipeline.SUCCESS

        if not self.supress_output_message:
            sys.stdout.write(self.current_status())
        return self.supress_exception

    def current_status(self) -> str:
        """
        Return a string message with current pipeline information.

        Message:
            - Name (str)
            - Started (datetime)
            - Finished (datetime)
            - Status (str). Options: PENDING, STARTED, SUCCESS, FAIL
            - Details (str)
        """
        msg = f"- Pipeline: {self.name}"
        msg += f"\n\t Started at: {self.start_time.isoformat()}"
        msg += f"\n\t Finished at: {self.end_time.isoformat()}"
        msg += f"\n\t Status: {self.status}"
        msg += f"\n\t Details: {self.detail}"
        return msg
