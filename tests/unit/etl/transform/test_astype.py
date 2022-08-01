import numpy

import pandas as pd

from tiny_blocks.etl.transform.astype import Astype
from tiny_blocks.etl.extract.from_sql_table import ExtractSQLTable


def test_sql_load_into_sqlite(sqlite_source, sqlite_sink):

    extract_sql = ExtractSQLTable(source=sqlite_source, table_name="test")
    as_type = Astype(dtype={"e": numpy.float})

    generator = as_type.get_iter(generator=extract_sql.get_iter())

    # exhaust and assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert df.dtypes
