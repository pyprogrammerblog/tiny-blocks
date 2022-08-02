import pandas as pd
import responses
from tiny_blocks.extract.from_sql_table import ExtractSQLTable
from tiny_blocks.transform.enrich_api import EnricherAPI, KwargsEnricherAPI


@responses.activate
def test_enrich_from_api(sqlite_source):

    responses.add(url="https://hola-mundo.com", json={"result": "Hola"})

    extract_sql = ExtractSQLTable(dsn_conn=sqlite_source, table_name="test")
    kwargs = KwargsEnricherAPI(default_value="default")
    enrich = EnricherAPI(
        url="https://hola-mundo.com",
        from_column="c",
        to_column="d",
        kwargs=kwargs,
    )

    generator = extract_sql.get_iter()
    generator = enrich.get_iter(generator=generator)

    # assert
    df = pd.concat(generator)
    assert df.shape == (3, 3)
    assert df.columns.to_list() == ["a", "b", "c", "d"]
    assert df.d.to_list() == ["Hola", "Hola", "default"]
