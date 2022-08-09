import logging
import sys
import traceback
from typing import List, Union, Iterator
from datetime import datetime

import pandas as pd

from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.extract.base import ExtractBase

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
        - max_retries: (str). Number of retries in case of Exception
        - supress_info: (bool). Supress info about the pipeline result
        - supress_exception: (bool). Supress Pipeline exception if it happens

    Usage:
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.load import ToSQL
        >>> from tiny_blocks import Pipeline
        >>>
        >>> from_csv = FromCSV(path='/path/to/file.csv')
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postgres://...')
        >>> fill_na = Fillna()
        >>>
        >>> with Pipeline(name="My Pipeline") as pipe:
        >>>     pipe >> from_csv >> fill_na >> to_sql
    """

    def __init__(
        self,
        name: str,
        description: str = None,
        max_retries: int = 3,
        supress_output_message: bool = False,
        supress_exception: bool = True,
    ):
        self.name: str = name
        self.description: str | None = description
        self.max_retries: int = max_retries
        self.supress_exception: bool = supress_exception
        self.supress_output_message: bool = supress_output_message
        self._status: str = Status.PENDING
        self._output_message: str = ""
        self._generator: List[Iterator[pd.DataFrame]]

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self._status = Status.STARTED
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            self.end_time = datetime.utcnow()
            self._status = Status.FAIL
        else:
            self._status = Status.SUCCESS
            self.end_time = datetime.utcnow()

        if not self.supress_output_message:
            sys.stdout.write(self.current_status())
        return self.supress_exception

    def current_status(self) -> str:
        """
        Return a string message with current pipeline information.
        """
        msg = f"- Pipeline: {self.name}"
        msg += f"\n\t Started at: {self.start_time.isoformat()}"
        msg += f"\n\t Finished at: {self.end_time.isoformat()}"
        msg += f"\n\t Status: {self._status}"
        return msg

    def __rshift__(
        self, next: Union[ExtractBase, TransformBase, LoadBase, "FanIn"]
    ) -> "Pipeline":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, FanIn):
            self._generator = next.get_iter()
            return self
        elif isinstance(next, ExtractBase):
            self._generator = [next.get_iter()]
            return self
        elif isinstance(next, TransformBase):
            self._generator = [next.get_iter(*self._generator)]
            return self
        elif isinstance(next, LoadBase):
            next.exhaust(*self._generator)
        else:
            raise ValueError("Unsupported Block Type")


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
        >>> with Pipeline(name="My Pipeline") as pipe:
        >>>     pipe >> FanIn(csv_1, csv_2)  >> merge >> to_sql
    """

    def __init__(self, *blocks: ExtractBase):
        self.blocks = blocks

    def get_iter(self) -> List[Iterator[pd.DataFrame]]:
        return [block.get_iter() for block in self.blocks]
