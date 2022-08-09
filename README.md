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
from tiny_blocks.extract import FromCSV
from tiny_blocks.transform import DropDuplicates
from tiny_blocks.transform import Fillna
from tiny_blocks.load import ToSQL
from tiny_blocks import Pipeline

# ETL Blocks
from_csv = FromCSV(path='/path/to/file.csv')
drop_duplicates = DropDuplicates()
fill_na = Fillna(value="Hola Mundo")
to_sql = ToSQL(dsn_conn='psycopg2+postgres://user:***@localhost:5432/foobar')

# Run the Pipeline
with Pipeline(name="The cool pipeline") as pipe:
    pipe >> from_csv >> drop_duplicates >> fill_na >> to_sql
```

Documentation
--------------

Please visit this [link](https://tiny-blocks.readthedocs.io/en/latest/) for documentation.
