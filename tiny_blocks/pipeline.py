import sys
from typing import Callable, Union
from datetime import datetime
from pydantic import BaseModel
import logging
import graphlib
from uuid import UUID, uuid4
from pydantic import Field
from typing import List, Set, Dict, NoReturn
from tiny_blocks.load.base import LoadBase
from tiny_blocks.extract.base import ExtractBase
from tiny_blocks.transform.base import TransformBase
from tiny_blocks.extract import ExtractBlocks
from tiny_blocks.transform import TransformBlocks
from tiny_blocks.load import LoadBlocks


Blocks = Union[ExtractBlocks | TransformBlocks | LoadBlocks]


__all__ = ["FanIn", "FanOut", "Pipeline"]


logger = logging.getLogger(__name__)


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

    def __init__(self, *sinks: LoadBase | "Sink"):
        self.sinks = sinks


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

    def __init__(self, *pipes: ExtractBase | "Sink"):
        self.pipes = pipes

    def __rshift__(
        self, next: TransformBase | LoadBase
    ) -> NoReturn | "SmartStream":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, TransformBase):
            smart_stream = SmartStream()
            smart_stream.graph |= {
                next.uuid: {pipe.uuid for pipe in self.pipes}
            }
            smart_stream.blocks.add(next)
            smart_stream.blocks.update(self.pipes)
            smart_stream.current_block = next
            return smart_stream
        elif isinstance(next, LoadBase):
            smart_stream = SmartStream()
            smart_stream.graph |= {
                next.uuid: {pipe.uuid for pipe in self.pipes}
            }
            smart_stream.blocks.add(next)
            smart_stream.blocks.update(self.pipes)
            smart_stream.current_block = next
            return smart_stream.exhaust(block=next)  # finish here
        else:
            raise ValueError("Unsupported Block Type")


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


class SmartStream(BaseModel):
    """
    SmartStream Model
    """

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(default="Stream", description="Stream name")
    description: str = Field(default=None, description="Description")

    created: datetime = Field(default_factory=datetime.utcnow)
    updated: datetime = Field(default_factory=datetime.utcnow)

    graph: Dict[UUID, Set[UUID]]
    blocks: Set[Blocks] = Field(default_factory=set, description="Blocks")
    current_block: Blocks

    def __str__(self):
        return f"SmartStream-{self.uuid}"

    def topological_order(self, until: UUID = None) -> tuple:
        """
        Return a tuple with the topological order of the graph.

        For example, the graph:
        {"D": {"B", "C"}, "C": {"A"}, "B": {"A"}}

             | -> B -> |
        A -> |         | -> D
             | -> C -> |

        returns:
        ['A', 'C', 'B', 'D'] or ['A', 'B', 'C', 'D']
        """
        ts = graphlib.TopologicalSorter(self.graph)
        return tuple(ts.static_order())

    def exhaust_multiple(self, *blocks: Blocks):

        for block in blocks:
            try:
                self.exhaust(block=block)
            except Exception as e:
                logger.error(str(e))

    def exhaust(self, block: Blocks):
        """
        When a sink is found execute!
        """
        generators = {}  # keep fresh generators
        blocks = {block.uuid: block for block in self.blocks}

        for uuid in self.topological_order(until=block.uuid):
            block = blocks[uuid]
            if isinstance(block, ExtractBase):
                generators[uuid] = block.get_iter()
            elif isinstance(block, TransformBase):
                source = [generators[i] for i in self.graph[uuid]]
                generators[uuid] = block.get_iter(source=source)
            elif isinstance(block, LoadBase):
                source = generators[self.graph[uuid]]
                return block.exhaust(source=source)

    def __rshift__(
        self, next: "TransformBase" | LoadBase | FanOut
    ) -> NoReturn | "SmartStream":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, (TransformBase, LoadBase)):
            self.graph |= {next.uuid: {self.current_block.uuid}}
            self.blocks.add(next)
            self.current_block = next
            return self
        elif isinstance(next, FanOut):
            self.graph |= {
                sink.uuid: {self.current_block.uuid} for sink in next.sinks
            }
            self.blocks.update(*next.sinks)
            return self
        else:
            raise ValueError("Unsupported Block Type")


class Sink(BaseModel):
    """
    Partial Streams (Sink) are generated by ExtractBlocks, FanOut
    without any Extract Block
    """

    missing: UUID
    graph: Dict[UUID, Set[UUID]]
    current_block: Blocks
    blocks: Set[Blocks] = Field(default_factory=set, description="Blocks")

    def __rshift__(
        self, next: "TransformBase" | LoadBase | FanOut
    ) -> NoReturn | "Sink":
        """
        The `>>` operator for the tiny-blocks library.
        """
        if isinstance(next, (TransformBase, LoadBase)):
            self.graph |= {next.uuid: {self.current_block.uuid}}
            self.blocks.add(next)
            self.current_block = next
            return self
        elif isinstance(next, FanOut):
            self.graph |= {
                sink.uuid: {self.current_block.uuid} for sink in next.sinks
            }
            self.blocks.update(next.sinks)
            self.current_block = next
            return self
        else:
            raise ValueError("Unsupported Block Type")
