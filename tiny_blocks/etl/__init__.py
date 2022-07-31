from typing import Union

# extract blocks
from tiny_blocks.extract.read_csv import ReadCSVBlock
from tiny_blocks.extract.read_json import ReadJSONBlock
from tiny_blocks.extract.read_sql import ReadSQLBlock

# load blocks
from tiny_blocks.load.to_csv import WriteCSVBlock
from tiny_blocks.load.to_json import WriteJSONBlock
from tiny_blocks.load.to_sql import WriteSQLBlock

# transform blocks
from tiny_blocks.transform.align import AlignBlock
from tiny_blocks.transform.append import AppendBlock
from tiny_blocks.transform.apply import ApplyBlock
from tiny_blocks.transform.applymap import ApplyMapBlock
from tiny_blocks.transform.drop_duplicates import (
    DropDuplicatesBlock,
)
from tiny_blocks.transform.fillna import FillnaBlock

# from blocks.groupby import GroupByBlock
# from blocks.join import JoinBlock
from tiny_blocks.transform.merge import MergeBlock

# from blocks.pivot_table import PivotTableBlock
# from blocks.query import QueryBlock
# from blocks.rename import RenameBlock
# from blocks.replace import ReplaceBlock
# from blocks.resample import ResampleBlock
# from blocks.sort_values import SortValuesBlock
# from blocks.where import WhereBlock
# import more models here...


# register new ETL Blocks here...
Blocks = Union[
    #
    # Extract Blocks
    ReadCSVBlock,
    ReadJSONBlock,
    ReadSQLBlock,
    #
    # Load Blocks
    WriteCSVBlock,
    WriteJSONBlock,
    WriteSQLBlock,
    #
    # Transform Blocks
    AlignBlock,
    AppendBlock,
    ApplyBlock,
    ApplyMapBlock,
    # AsTypeBlock,
    # ConcatBlock,
    # DropBlock,
    DropDuplicatesBlock,
    # DropnaBlock,
    FillnaBlock,
    # GroupByBlock,
    # JoinBlock,
    MergeBlock,
    # PivotTableBlock,
    # QueryBlock,
    # RenameBlock,
    # ReplaceBlock,
    # ResampleBlock,
    # SortValuesBlock,
    # WhereBlock,
]
