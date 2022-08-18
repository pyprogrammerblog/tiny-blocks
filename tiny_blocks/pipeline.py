import logging
import sys
from typing import List, Callable, Iterator, Union, NoReturn
from datetime import datetime

import pandas as pd

from tiny_blocks.extract.base import ExtractBase
from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase

__all__ = ["Pipeline", "FanIn"]


logger = logging.getLogger(__name__)


class Status:
    PENDING: str = "PENDING"
    STARTED: str = "STARTED"
    SUCCESS: str = "SUCCESS"
    FAIL: str = "FAIL"


class Pipeline:
    """
    Defines the class gluing all Pipeline Blocks

    Params:
        - name: (str). Name of the Pipeline
        - description: (str). Description of the Pipeline
        - supress_info: (bool). Supress info about the pipeline result
        - supress_exception: (bool). Supress Pipeline exception if it happens

    Usage:
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.load import ToSQL
        >>> from tiny_blocks import Pipeline
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> fill_na = Fillna(value="Hola Mundo")
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postgres://...')
        >>>
        >>> with Pipeline(name="My Pipeline") as pipe:
        >>>     from_csv >> fill_na >> to_sql
    """

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
        self.status: str = Status.PENDING
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.detail: str = ""
        self._callables: List = [Callable]

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.status = Status.STARTED
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.utcnow()
        if exc_type:
            self.detail = f"Failure: {exc_val}\n"
            self.status = Status.FAIL
        else:
            self.status = Status.SUCCESS

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

    def __init__(self, *blocks: Union[ExtractBase, "Pipe"]):
        self.blocks = blocks

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
        return [block.get_iter() for block in self.blocks]
