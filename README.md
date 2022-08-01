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
from pathlib import Path
from tiny_blocks.etl.extract.from_csv import ExtractCSV
from tiny_blocks.etl.load.to_sql import LoadSQL
from tiny_blocks.etl.transform.drop_duplicates import DropDuplicates
from tiny_blocks.etl.transform.fillna import Fillna

from tiny_blocks.sources.csv import CSVSource
from tiny_blocks.sinks.sql import SQLSink

# settings
conn_string = 'psycopg2+postgres://user:pass@localhost:5432/foobar'
csv_path = Path('/path/to/file.csv')

# Source and Sink
source = CSVSource(path=csv_path)
sink = SQLSink(connection_string=conn_string)

# ETL Blocks
extract_from_csv = ExtractCSV(source=source)
drop_duplicates = DropDuplicates()
fill_na = Fillna()
load_to_sql = LoadSQL(sink=sink)

# Pipeline
extract_from_csv >> drop_duplicates >> fill_na >> load_to_sql
```

Documentation
--------------

Please visit ... :TODO
