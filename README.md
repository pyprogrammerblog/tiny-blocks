 tiny-blocks
=============

[![Documentation Status](https://readthedocs.org/projects/tiny-blocks/badge/?version=latest)](https://tiny-blocks.readthedocs.io/en/latest/?badge=latest)
[![License-MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pyprogrammerblog/tiny-blocks/blob/master/LICENSE)
[![GitHub Actions](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)
[![PyPI version](https://badge.fury.io/py/tiny-blocks.svg)](https://badge.fury.io/py/tiny-blocks)

Tiny Blocks to build large and complex ETL data pipelines!

Tiny-Blocks is a library for **data engineering** operations. 
Each **pipeline** is made out of **tiny-blocks** glued with the `>>` operator.
This library relies on a fundamental streaming abstraction consisting of three
parts: **extract**, **transform**, and **load**. You can view a pipeline 
as an extraction, followed by zero or more transformations, followed by a sink. 
Visually, this looks like:

```
extract -> transform1 -> transform2 -> ... -> transformN -> load
```

You can also `fan-in`, `fan-out` for more complex operations.

```
extract1 -> transform1 -> |-> transform2 -> ... -> | -> transformN -> load1
extract2 ---------------> |                        | -> load2
```

Tiny-Blocks use **generators** to stream data. Each **chunk** is a **Pandas DataFrame**. 
The `chunksize` or buffer size is adjustable per pipeline.

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

# Pipeline
from_csv >> fill_na >> to_sql
```

Examples
----------------------

For more complex examples please visit 
the [notebooks' folder](https://github.com/pyprogrammerblog/tiny-blocks/blob/master/notebooks/Examples.ipynb).


Documentation
--------------

Please visit this [link](https://tiny-blocks.readthedocs.io/en/latest/) for documentation.
