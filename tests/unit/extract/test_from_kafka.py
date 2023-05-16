from tiny_blocks.extract.from_kafka import FromKafka


def test_extract_from_kafka(kafka_source):

    from_kafka = FromKafka(kafka_source)
    generator = from_kafka.get_iter()

    # exhaust the generator
    data = list(generator)

    # assertions
    assert len(data) == 4
    assert data[0].columns() == ["name", "age"]
    assert data[0].values() == ["Mateo", 30]
