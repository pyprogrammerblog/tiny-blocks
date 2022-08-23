from tiny_blocks.pipeline import FanOut, Tee
from tiny_blocks.extract import FromCSV
from tiny_blocks.load import ToCSV
from tiny_blocks.transform import Apply
import pandas as pd


def test_complex_flows_with_tee(delete_csv_sinks):

    data = {"A": [1, 1, 1]}
    pd.DataFrame(data=data).to_csv(
        "/code/tests/data/source1.csv", sep="|", index=False
    )
    from_csv = FromCSV(path="/code/tests/data/source1.csv")

    to_csv_1 = ToCSV(path="/code/tests/data/sink1.csv")
    to_csv_2 = ToCSV(path="/code/tests/data/sink2.csv")
    to_csv_3 = ToCSV(path="/code/tests/data/sink3.csv")

    apply = Apply(apply_to_column="A", set_to_column="A", func=lambda x: x + 1)

    """
          |-> csv1 (+0)         |-> csv2 (+2)
    csv ->|-> apply -> apply -> |-> csv3 (+2)
    """
    from_csv >> FanOut(to_csv_1) >> apply >> apply >> Tee(to_csv_2, to_csv_3)

    assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 1
    assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 3
    assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3

    """
          |-> csv2 (+0)
          |-> csv1 (+0)
    csv ->|-> apply -> apply -> csv3 (+2)
    """
    from_csv >> FanOut(to_csv_1, to_csv_2) >> apply >> apply >> to_csv_3

    assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 1
    assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 1
    assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3


def test_complex_flows_with_tee_2(delete_csv_sinks):

    data = {"A": [1, 1, 1]}
    pd.DataFrame(data=data).to_csv(
        "/code/tests/data/source1.csv", sep="|", index=False
    )
    from_csv = FromCSV(path="/code/tests/data/source1.csv")

    to_csv_1 = ToCSV(path="/code/tests/data/sink1.csv")
    to_csv_2 = ToCSV(path="/code/tests/data/sink2.csv")
    to_csv_3 = ToCSV(path="/code/tests/data/sink3.csv")

    apply = Apply(apply_to_column="A", set_to_column="A", func=lambda x: x + 1)

    (
        from_csv
        >> FanOut(apply >> to_csv_1)
        >> apply
        >> apply
        >> Tee(to_csv_2, to_csv_3)
    )

    assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 2
    assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 3
    assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3


def test_complex_flows_with_tee_3(delete_csv_sinks):

    data = {"A": [1, 1, 1]}
    pd.DataFrame(data=data).to_csv(
        "/code/tests/data/source1.csv", sep="|", index=False
    )
    from_csv = FromCSV(path="/code/tests/data/source1.csv")

    to_csv_1 = ToCSV(path="/code/tests/data/sink1.csv")
    to_csv_2 = ToCSV(path="/code/tests/data/sink2.csv")
    to_csv_3 = ToCSV(path="/code/tests/data/sink3.csv")

    apply = Apply(apply_to_column="A", set_to_column="A", func=lambda x: x + 1)

    (
        from_csv
        >> FanOut(apply >> apply >> to_csv_1)
        >> apply
        >> apply
        >> Tee(to_csv_2, to_csv_3)
    )

    assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 3
    assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 3
    assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3


def test_complex_flows_with_tee_4(delete_csv_sinks):

    data = {"A": [1, 1, 1]}
    pd.DataFrame(data=data).to_csv(
        "/code/tests/data/source1.csv", sep="|", index=False
    )
    from_csv = FromCSV(path="/code/tests/data/source1.csv")

    to_csv_1 = ToCSV(path="/code/tests/data/sink1.csv")
    to_csv_2 = ToCSV(path="/code/tests/data/sink2.csv")
    to_csv_3 = ToCSV(path="/code/tests/data/sink3.csv")

    apply = Apply(apply_to_column="A", set_to_column="A", func=lambda x: x + 1)

    (
        from_csv
        >> FanOut(apply >> apply >> to_csv_1)
        >> apply
        >> apply
        >> Tee(apply >> to_csv_2, to_csv_3)
    )

    assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 3
    assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 4
    assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 3


def test_complex_flows_with_tee_5(delete_csv_sinks):

    data = {"A": [1, 1, 1]}
    pd.DataFrame(data=data).to_csv(
        "/code/tests/data/source1.csv", sep="|", index=False
    )
    from_csv = FromCSV(path="/code/tests/data/source1.csv")

    to_csv_1 = ToCSV(path="/code/tests/data/sink1.csv")
    to_csv_2 = ToCSV(path="/code/tests/data/sink2.csv")
    to_csv_3 = ToCSV(path="/code/tests/data/sink3.csv")

    apply = Apply(apply_to_column="A", set_to_column="A", func=lambda x: x + 1)

    (
        from_csv
        >> FanOut(apply >> apply >> to_csv_1)
        >> apply
        >> apply
        >> Tee(apply >> apply >> to_csv_2, apply >> to_csv_3)
    )

    assert pd.read_csv(to_csv_1.path, sep="|").iloc[0, 0] == 3
    assert pd.read_csv(to_csv_2.path, sep="|").iloc[0, 0] == 5
    assert pd.read_csv(to_csv_3.path, sep="|").iloc[0, 0] == 4
