# import pytest
# from biofilter.biofilter.db.models import Group
# from biofilter.biofilter import Biofilter
# import pandas as pd


# @pytest.fixture
# def biofilter():
#     return Biofilter("sqlite:///biofilter/biofilter.sqlite")


# def test_query_group_all_df(biofilter):
#     results = biofilter.query(Group, type="df")
#     assert isinstance(results, pd.DataFrame)


# def test_query_group_all(biofilter):
#     results = biofilter.query(Group)
#     assert isinstance(results, list)
#     assert all(isinstance(r, Group) for r in results)


# def test_query_group_by_filter(biofilter):
#     results = biofilter.query(Group, filters={"label": "biogrid:103"})
#     assert isinstance(results, list)
#     assert all(isinstance(r, Group) for r in results)
#     for r in results:
#         assert r.label == "biogrid:103"


# def test_query_group_like(biofilter):
#     results = biofilter.query(Group, filters={"label__like": "biogrid%"})
#     assert all("biogrid" in r.label for r in results)


# def test_query_group_in(biofilter):
#     results = biofilter.query(Group, filters={"source_id__in": [1, 2]})
#     assert all(r.source_id in [1, 2] for r in results)


# def test_query_group_range(biofilter):
#     results = biofilter.query(Group, filters={"group_id__between": (10, 20)})
#     assert all(10 <= r.group_id <= 20 for r in results)