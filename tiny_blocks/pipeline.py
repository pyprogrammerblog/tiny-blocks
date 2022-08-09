import logging
import sys
import traceback
from functools import reduce
from typing import List, Callable, Union
from datetime import datetime

from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.extract.base import ExtractBase

__all__ = ["Pipeline", "FanIn"]


logger = logging.getLogger(__name__)


class Status:
    PENDING: str = "PENDING"
    STARTED: str = "STARTED"
    RETRY: str = "RETRY"
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
        >>> from tiny_blocks.transform import DropDuplicates
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
        self._count_retries: int = 0
        self._output_message: str = ""
        self._callables: List[Callable] = []

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self._status = Status.STARTED
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self._status = Status.SUCCESS
            self.end_time = datetime.utcnow()
            if not self.supress_output_message:
                sys.stdout.write(self.current_status())
        else:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self._count_retries < self.max_retries:
                self._count_retries += 1
                self._status = Status.RETRY
            else:
                self.end_time = datetime.utcnow()
                self._status = Status.FAIL
                if not self.supress_output_message:
                    sys.stdout.write(self.current_status())
                return self.supress_exception

    def current_status(self) -> str:
        """
        Return a sstring message with current pipeline information.
        """
        msg = f"- Pipeline: {self.name}"
        msg += f"\n\t Started: {self.start_time.isoformat()}"
        msg += f"\n\t Finished: {self.end_time.isoformat()}"
        msg += f"\n\t Status: {self._status}"
        msg += f"\n\t Number of retries: {self._count_retries}"
        return msg

    def __rshift__(
        self, next: Union[ExtractBase, TransformBase, LoadBase, "FanIn"]
    ) -> "Pipeline":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, ExtractBase | TransformBase | FanIn):
            self._callables.append(next.get_iter)  # append signatures
            return self
        elif isinstance(next, LoadBase):
            generator = reduce(lambda f, g: g(*f), list(self._callables))
            next.exhaust(generator=generator)
        else:
            raise ValueError("Unsupported Block Type")


class FanIn:
    """
    Gather multiple operations and send them to the next block.
    The next block must accept multiple arguments, like for example:
    ``tiny_blocks.tranform.Merge``

    Usage:
        >>> from tiny_blocks.extract import FromCSV
        >>> from tiny_blocks.load import ToSQL
        >>> from tiny_blocks import FanIn, Pipeline
        >>> from tiny_blocks.transform import Merge
        >>>
        >>> from_csv_1 = FromCSV(path='/path/to/file1.csv')
        >>> from_csv_2 = FromCSV(path='/path/to/file2.csv')
        >>> merge = Merge(left_on="A", right_on="B", how="inner")
        >>> to_sql = ToSQL(dsn_conn='psycopg2+postgres://...')
        >>>
        >>> with Pipeline(name="My Pipeline") as pipe:
        >>>     pipe >> FanIn(from_csv_1, from_csv_2)  >> merge >> to_sql
    """

    def __init__(self, *blocks: ExtractBase | TransformBase):
        self.blocks = blocks

    def get_iter(self) -> List[Callable]:
        return [block.get_iter for block in self.blocks]
