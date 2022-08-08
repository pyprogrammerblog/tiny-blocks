import logging
import sys
import traceback
from functools import reduce
from typing import List, Callable, Union
from datetime import datetime

from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.extract.base import ExtractBase

__all__ = ["FanIn", "Pipeline"]


logger = logging.getLogger(__name__)


class Status:
    PENDING: str = "PENDING"
    STARTED: str = "STARTED"
    RETRY: str = "RETRY"
    SUCCESS: str = "SUCCESS"
    FAIL: str = "FAIL"


class Pipeline:
    """
    Defines the Pipeline that glues all Pipeline Blocks

    Params:
        name: (str). Name of the Pipeline
        description: (str). Description of the Pipeline
        max_retries: (str). Number of retries in case of Exception
        supress_info: (bool). Supress info about the pipeline result
        supress_exception: (bool). Supress Pipeline exception if it happens

    Usage:
        >>> from tiny_blocks.extract import ExtractCSV
        >>> from tiny_blocks.transform import DropDuplicates
        >>> from tiny_blocks.transform import Fillna
        >>> from tiny_blocks.load import LoadSQL
        >>> from tiny_blocks import Pipeline

        # ETL Blocks
        >>> from_csv = ExtractCSV(path='/path/to/file.csv')
        >>> to_sql = LoadSQL(dsn_conn='psycopg2+postgres://...')
        >>> fill_na = Fillna()  # fill None values

        # Pipeline
        >>> with Pipeline(name="My Pipeline") as pipe:
        >>>     pipe >> from_csv >> fill_na >> to_sql

        - Pipeline: My Pipeline
            Started: 2022-08-08T16:11:30.134018
            Finished: 2022-08-08T16:11:35.134018
            Status: SUCCESS
            Number of retries: 0
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
        self._tasks: List[Callable] = []

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
        Current output
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
            self._tasks.append(next.get_iter)  # append signatures
            return self
        elif isinstance(next, LoadBase):
            generator = reduce(lambda f, g: g(*f), list(self._tasks))
            next.exhaust(generator=generator)
        else:
            raise ValueError("Unsupported Block Type")


class FanIn:
    def __init__(self, *blocks: ExtractBase | TransformBase):
        self.blocks = blocks

    def get_iter(self) -> List[Callable]:
        return [block.get_iter for block in self.blocks]
