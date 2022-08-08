 tiny-blocks
=============

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
from tiny_blocks.extract import ExtractCSV
from tiny_blocks.transform import DropDuplicates
from tiny_blocks.transform import Fillna
from tiny_blocks.load import LoadSQL
from tiny_blocks import Pipeline

# ETL Blocks
extract_from_csv = ExtractCSV(path='/path/to/file.csv')
drop_duplicates = DropDuplicates()
fill_na = Fillna()
load_to_sql = LoadSQL(dsn_conn='psycopg2+postgres://user:***@localhost:5432/foobar')

# Pipeline
with Pipeline(name="My Pipeline") as pipe:
    pipe >> extract_from_csv >> drop_duplicates >> fill_na >> load_to_sql
```

Documentation
--------------

Please visit this [link](https://tiny-blocks.readthedocs.io/en/latest/) for documentation.
