# from tiny_blocks.transform.sort import Sort
#
#
# def test_my_test(postgres_source, csv_source):
#     """ """
#     pass
#
#
# def test_sort_by_descending_name(source_data):
#     sort = Sort(by=["name"])
#     generator = sort.get_iter(source=source_data)
#
#     sorted_data = list(generator)
#     assert sorted_data
#
#
# def test_sort_by_ascending_name(source_data):
#     sort = Sort(by=["name"], ascending=False)
#     generator = sort.get_iter(source=source_data)
#
#     sorted_data = list(generator)
#     assert sorted_data
#
#
# def test_sort_by_ascending_age(source_data):
#     sort = Sort(by=["age"])
#     generator = sort.get_iter(source=source_data)
#
#     sorted_data = list(generator)
#     assert sorted_data
#
#
# def test_sort_by_descending_age(source_data):
#     sort = Sort(by=["age"], ascending=False)
#     generator = sort.get_iter(source=source_data)
#
#     sorted_data = list(generator)
#     assert sorted_data
