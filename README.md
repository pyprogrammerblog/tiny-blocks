 tiny-blocks
=============

[![Documentation Status](https://readthedocs.org/projects/tiny-blocks/badge/?version=latest)](https://tiny-blocks.readthedocs.io/en/latest/?badge=latest)
[![License-MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pyprogrammerblog/tiny-blocks/blob/master/LICENSE)
[![GitHub Actions](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)](https://github.com/pyprogrammerblog/tiny-blocks/workflows/CI/badge.svg/)
[![PyPI version](https://badge.fury.io/py/tiny-blocks.svg)](https://badge.fury.io/py/tiny-blocks)

Tiny Blocks to build large and complex pipelines!

Tiny-Blocks is a library for streaming operations, composed using the `>>`
operator. This allows for easy extract, transform and load operations.

### Pipeline Components: Sources, Pipes, and Sinks
This library relies on a fundamental streaming abstraction consisting of three
parts: extract, transform, and load. You can view a pipeline as a extraction, followed
by zero or more transformations, followed by a sink. Visually, this looks like:

```
source >> pipe1 >> pipe2 >> pipe3 >> ... >> pipeN >> sink
```

Installation
-------------

Install it using ``pip``

```shell
pip install tiny-blocks
```

Basic usage example
--------------------

```python
from tiny_blocks.extract import FromCSV
from tiny_blocks.transform import DropDuplicates
from tiny_blocks.transform import Fillna
from tiny_blocks.load import ToSQL

# ETL Blocks
from_csv = FromCSV(path='/path/to/file.csv')
drop_duplicates = DropDuplicates()
fill_na = Fillna(value="Hola Mundo")
to_sql = ToSQL(dsn_conn='psycopg2+postgres://...')

# Run the Pipeline
from_csv >> drop_duplicates >> fill_na >> to_sql
```

Documentation
--------------

Please visit this [link](https://tiny-blocks.readthedocs.io/en/latest/) for documentation.
