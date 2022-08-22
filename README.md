 tiny-blocks
=============

[![Documentation Status](https://readthedocs.org/projects/tiny-blocks/badge/?version=latest)](https://tiny-blocks.readthedocs.io/en/latest/?badge=latest)
[![License-MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pyprogrammerblog/tiny-blocks/blob/master/LICENSE)
[![GitHub Actions](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)
[![PyPI version](https://badge.fury.io/py/tiny-blocks.svg)](https://badge.fury.io/py/tiny-blocks)

Tiny Blocks to build large and complex pipelines!

Tiny-Blocks is a library for **data engineering** operations. 
Each pipeline is made out of blocks glued with the `>>` operator. 
This allows for easy extract, transform and load operations.

Tiny-Blocks use **generators** to stream data. The `chunksize` or buffer size 
is adjustable per extraction or loading operation.

### Pipeline Components: Sources, Pipes, and Sinks
This library relies on a fundamental streaming abstraction consisting of three
parts: extract, transform, and load. You can view a pipeline as an extraction, followed
by zero or more transformations, followed by a sink. Visually, this looks like:

```
extract -> transform1 -> transform2 -> ... -> transformN >> load
```

You can also `fan-in`, `fan-out` or `tee` for more complex operations.

```
extract1 -> transform1 -> |-> transform2 -> ... -> | -> transformN >> load1
extract2 ---------------> |                        | -> load2
```


Installation
-------------

Install it using ``pip``

```shell
pip install tiny-blocks
```

Usage examples
---------------

```python
from tiny_blocks.extract import FromCSV, FromSQLTable
from tiny_blocks.transform import DropDuplicates, Fillna, Merge
from tiny_blocks.load import ToSQL, ToCSV
from tiny_blocks.pipeline import Tee, FanIn

# ETL Blocks
from_csv = FromCSV(path='/path/to/source.csv')
from_sql = FromSQLTable(dsn_conn='psycopg2+postgres://...', table_name="source")
merge = Merge(left_on="Column A", right_on="Column B", how="left")
fill_na = Fillna(value="Hola Mundo")
drop_dupl = DropDuplicates()
to_sql = ToSQL(dsn_conn='psycopg2+postgres://...', table_name="sink")
to_csv = ToCSV(path='/path/to/sink.csv')

# Run a simple Pipeline
from_csv >> drop_dupl >> fill_na >> to_sql

# Or a more complex one  
# read_sql -> |                                | -> write into csv
# read csv -> | -> merge -> drop duplicates -> | -> fill null values -> write to SQL
FanIn(from_csv, from_sql) >> merge >> drop_dupl >> Tee(to_csv, fill_na >> to_sql)
```

Examples
---------

For more complex examples please visit 
the [notebooks' folder](https://github.com/pyprogrammerblog/tiny-blocks/tree/master/notebooks).


Documentation
--------------

Please visit this [link](https://tiny-blocks.readthedocs.io/en/latest/) for documentation.
