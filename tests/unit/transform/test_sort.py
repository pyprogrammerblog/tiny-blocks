import tempfile
import pandas as pd
from tiny_blocks.extract.from_csv import FromCSV
from tiny_blocks.transform.sort import Sort


def test_sort_by_ascending():

    with tempfile.NamedTemporaryFile(suffix=".csv") as file:

        data = {"a": [6, 4, 1], "b": [1, 3, 4], "c": [2, 3, 1]}
        pd.DataFrame(data=data).to_csv(file.name, sep="|", index=False)

        extract_csv = FromCSV(path=file.name)

        # by a, ascending
        sort = Sort(by=["a"])
        generator = extract_csv.get_iter()
        generator = sort.get_iter(generator=generator)
        df = pd.concat(generator)
        assert df.shape == (3, 3)
        assert df.a.to_list() == [1, 4, 6]

        # by b, ascending
        sort = Sort(by=["b"], ascending=False)
        generator = extract_csv.get_iter()
        generator = sort.get_iter(generator=generator)
        df = pd.concat(generator)
        assert df.shape == (3, 3)
        assert df.a.to_list() == [1, 4, 6]

        # by c, descending
        sort = Sort(by=["c"], ascending=False)
        generator = extract_csv.get_iter()
        generator = sort.get_iter(generator=generator)
        df = pd.concat(generator)
        assert df.shape == (3, 3)
        assert df.c.to_list() == [3, 2, 1]

        # by c, descending
        sort = Sort(by=["c"], ascending=True)
        generator = extract_csv.get_iter()
        generator = sort.get_iter(generator=generator)
        df = pd.concat(generator)
        assert df.shape == (3, 3)
        assert df.c.to_list() == [1, 2, 3]
