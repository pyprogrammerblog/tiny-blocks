 tiny-blocks
=============

[![Documentation Status](https://readthedocs.org/projects/tiny-blocks/badge/?version=latest)](https://tiny-blocks.readthedocs.io/en/latest/?badge=latest)
[![License-MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pyprogrammerblog/tiny-blocks/blob/master/LICENSE)
[![GitHub Actions](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)
[![PyPI version](https://badge.fury.io/py/tiny-blocks.svg)](https://badge.fury.io/py/tiny-blocks)

Tiny Blocks to build large and complex ETL pipelines!

Tiny-Blocks is a library for **data engineering** operations. 
Each pipeline is made out of blocks glued with the `>>` operator. 

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

Basic usage
---------------

```python
from tiny_blocks.extract import FromCSV
from tiny_blocks.transform import Fillna
from tiny_blocks.load import ToSQL

# ETL Blocks
from_csv = FromCSV(path='/path/to/source.csv')
fill_na = Fillna(value="Hola Mundo")
to_sql = ToSQL(dsn_conn='psycopg2+postgres://...', table_name="sink")

# Run a simple Pipeline
from_csv >> fill_na >> to_sql
```

Complex examples
----------------------

For more complex examples please visit 
the [notebooks' folder](https://github.com/pyprogrammerblog/tiny-blocks/tree/master/notebooks).


Documentation
--------------

Please visit this [link](https://tiny-blocks.readthedocs.io/en/latest/) for documentation.
