import logging
from datetime import datetime
from tiny_blocks.load.base import LoadBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.extract.base import ExtractBase

__all__ = ["FanIn", "Pipeline"]


logger = logging.getLogger(__name__)


class Pipeline:
    """
    Represent the glue between all operations in an ETL Operation
    """

    def __init__(self, name: str, supress_exception: bool = False):
        self.name: str
        self.description: str | None = None

    def __str__(self):
        return f"Task-{self.uuid}"

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.status = Status.STARTED
        # self.save()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self.count_retries < self.max_retries or settings.max_retries:
                self.count_retries += 1
                self.status = Status.RETRY
            else:
                self.end_time = datetime.utcnow()
                self.status = Status.FAIL
            self.save()
            return True

    def __rshift__(
        self, next: ExtractBase | TransformBase | LoadBase | "FanIn"
    ) -> "Pipeline":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, ExtractBase):
            self._prev_gen = [next.get_iter()]
            return self
        elif isinstance(next, TransformBase):
            generator = next.get_iter(*self._generator)
            return Pipeline(generator)
        elif isinstance(next, LoadBase):
            next.exhaust(*self.generator)
        else:
            raise ValueError("Think about this")


class FanIn:
    def __init__(self, *args: ExtractBase | Pipeline):
        self.generators = []
        for arg in args:
            if isinstance(arg, ExtractBase):
                self.generators.append(arg.get_iter())
            elif isinstance(arg, Stream):
                self.generators.append(*arg.generator)
            else:
                raise ValueError("Thinks about this")

    def __rshift__(self, next: TransformBase | LoadBase):
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            generator = next.get_iter(*self.generators)
            return Stream(generator)
        elif isinstance(next, LoadBase):
            next.exhaust(*self.generators)
        else:
            raise ValueError("Wrong block type")


class FanOut:
    pass
