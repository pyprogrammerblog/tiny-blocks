 tiny-blocks
=============

Tiny Blocks to build large and complex pipelines!

This module defines a library for streaming operations, composed using the `>>`
operator. This allows for easy to read and write shell-like pipelines.

### Pipeline Components: Sources, Pipes, and Sinks
This library relies on a fundamental streaming abstraction consisting of three
parts: sources, pipes, and sinks. You can view a pipeline as a source, followed
by zero or more pipes, followed by a sink. Visually, this looks like:

```python
    source >> pipe1 >> pipe2 >> ... >> pipeN >> sink
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
from tiny_blocks.extract import ExtractCSV
from tiny_blocks.transform import DropDuplicates
from tiny_blocks.transform import Fillna
from tiny_blocks.load import LoadSQL

# ETL Blocks
extract_from_csv = ExtractCSV(dsn_conn='psycopg2+postgres://user:***@localhost:5432/foobar')
load_to_sql = LoadSQL(path='/path/to/file.csv')
drop_duplicates = DropDuplicates()
fill_na = Fillna()

# Pipeline
extract_from_csv >> drop_duplicates >> fill_na >> load_to_sql
```

Documentation
--------------

Please visit ... :TODO
