import mock
import pandas as pd
from tiny_blocks.extract.from_sql_table import FromSQLTable
from tiny_blocks.transform.enrich_api import EnricherAPI, KwargsEnricherAPI


class ResponseOk:
    @property
    def ok(self):
        return True

    def json(self):
        return {"result": "Hola"}


class ResponseNotOk:
    @property
    def ok(self):
        return False


def test_enrich_from_api(sqlite_source):

    with mock.patch("requests.sessions.Session.get") as req:
        req.side_effect = [ResponseOk(), ResponseOk(), ResponseNotOk()]

        extract_sql = FromSQLTable(dsn_conn=sqlite_source, table_name="test")
        kwargs = KwargsEnricherAPI(default_value="default")
        enrich = EnricherAPI(
            url="https://hola-mundo.com",
            from_column="e",
            to_column="f",
            kwargs=kwargs,
        )

        generator = extract_sql.get_iter()
        generator = enrich.get_iter(generator=generator)

        # assert
        df = pd.concat(generator)
        assert df.shape == (3, 3)
        assert df.columns.to_list() == ["d", "e", "f"]
        assert df.f.to_list() == ["Hola", "Hola", "default"]
