 tiny-blocks
=============

Tiny Blocks to build large and complex pipelines!

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
