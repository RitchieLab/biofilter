import pytest
from unittest.mock import MagicMock, call
from biofilter_modules.mixins.input_group_mixin import GroupInputMixin


class TestGroupInputMixin:
    @pytest.fixture
    def mixin(self):
        mixin = GroupInputMixin()
        mixin._loki = MagicMock()
        mixin._options = MagicMock()
        mixin._inputFilters = {"test_db": {"group": 0}}
        return mixin

    def test_unionInputGroups(self, mixin):
        """
        Test the unionInputGroups method to ensure correct internal calls and
        tally handling.
        """
        db = "test_db"
        group_list = [("namespace", "name", "extra")]
        mixin._options.allow_ambiguous_groups = "no"

        # Define `generateGroupIDsByIdentifiers` with a tally dictionary
        def generate_groups_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["zero"] = 1
            tally["many"] = 1
            return [(1, 2, 3, 4)]

        # Mock `generateGroupIDsByIdentifiers`
        mixin._loki.generateGroupIDsByIdentifiers = MagicMock(
            side_effect=generate_groups_with_tally
        )
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        # Simula `executemany` para retornar dados, ativando o loop
        mixin._loki._biofilter.db.cursor().executemany.return_value = [(1,)]

        # Call the unionInputGroups method
        mixin.unionInputGroups(db, group_list)

        # Verify internal calls
        mixin.prepareTableForUpdate.assert_called_once_with(db, "group")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db group filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 groups\n")
        mixin.warn.assert_has_calls(
            [
                call("WARNING: ignored 1 unrecognized group identifier(s)\n"),
                call("WARNING: ignored 1 ambiguous group identifier(s)\n"),
            ],
            any_order=True,
        )
        assert mixin._inputFilters["test_db"]["group"] == 1

    def test_unionInputGroups_with_ambiguous_groups(self, mixin):
        """
        Test unionInputGroups method when `allow_ambiguous_groups` is "yes".
        """
        db = "test_db"
        group_list = [("namespace", "name", "extra")]
        mixin._options.allow_ambiguous_groups = "yes"

        # Define `generateGroupIDsByIdentifiers` with a tally dictionary
        def generate_groups_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["zero"] = 0
            tally["many"] = 1
            return [(1, 2, 3, 4)]

        # Mock `generateGroupIDsByIdentifiers`
        mixin._loki.generateGroupIDsByIdentifiers = MagicMock(
            side_effect=generate_groups_with_tally
        )
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        # Simula `executemany` para retornar dados, ativando o loop
        mixin._loki._biofilter.db.cursor().executemany.return_value = [(1,)]

        # Call the unionInputGroups method
        mixin.unionInputGroups(db, group_list)

        # Verify internal calls
        mixin.prepareTableForUpdate.assert_called_once_with(db, "group")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db group filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 groups\n")
        mixin.warn.assert_called_once_with(
            "WARNING: added multiple results for 1 ambiguous group identifier(s)\n"  # noqa E501
        )
        assert mixin._inputFilters["test_db"]["group"] == 1

    def test_intersectInputGroups(self, mixin):
        """
        Test the intersectInputGroups method to ensure correct behavior and
        tally handling.
        """
        db = "test_db"
        group_list = [("namespace", "name", "extra")]
        mixin._options.allow_ambiguous_groups = "no"

        # Define `generateGroupIDsByIdentifiers` with a tally dictionary
        def generate_groups_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["zero"] = 1
            tally["many"] = 1
            return [(1, 2, 3, 4)]

        # Mock `generateGroupIDsByIdentifiers`
        mixin._loki.generateGroupIDsByIdentifiers = MagicMock(
            side_effect=generate_groups_with_tally
        )
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin._inputFilters[db]["group"] = 1  # Init filter to bypass union
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 1

        # Call the intersectInputGroups method
        mixin.intersectInputGroups(db, group_list)

        # Verify internal calls
        mixin.prepareTableForQuery.assert_called_once_with(db, "group")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db group filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 groups (1 dropped)\n"
        )  # noqa E501
        mixin.warn.assert_has_calls(
            [
                call("WARNING: ignored 1 unrecognized group identifier(s)\n"),
                call("WARNING: ignored 1 ambiguous group identifier(s)\n"),
            ],
            any_order=True,
        )
        assert mixin._inputFilters[db]["group"] == 2

    def test_intersectInputGroups_with_ambiguous_groups(self, mixin):
        """
        Test intersectInputGroups method when `allow_ambiguous_groups` is "yes"
        """
        db = "test_db"
        group_list = [("namespace", "name", "extra")]
        mixin._options.allow_ambiguous_groups = "yes"

        # Define `generateGroupIDsByIdentifiers` with a tally dictionary
        def generate_groups_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["zero"] = 0
            tally["many"] = 1
            return [(1, 2, 3, 4)]

        # Mock `generateGroupIDsByIdentifiers`
        mixin._loki.generateGroupIDsByIdentifiers = MagicMock(
            side_effect=generate_groups_with_tally
        )
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin._inputFilters[db]["group"] = 1  # Init filter to bypass union
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 1

        # Call the intersectInputGroups method
        mixin.intersectInputGroups(db, group_list)

        # Verify internal calls
        mixin.prepareTableForQuery.assert_called_once_with(db, "group")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db group filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 groups (1 dropped)\n"
        )  # noqa E501
        mixin.warn.assert_called_once_with(
            "WARNING: kept multiple results for 1 ambiguous group identifier(s)\n"  # noqa E501
        )
        assert mixin._inputFilters[db]["group"] == 2

    def test_intersectInputGroups_triggers_union(self, mixin):
        """
        Test that intersectInputGroups triggers unionInputGroups if filter is
        not initialized.
        """
        db = "test_db"
        group_list = [("namespace", "name", "extra")]

        # Mock `unionInputGroups`
        mixin.unionInputGroups = MagicMock()

        # Call the intersectInputGroups method
        mixin.intersectInputGroups(db, group_list)

        # Verify that unionInputGroups was called instead
        mixin.unionInputGroups.assert_called_once_with(db, group_list, None)

    def test_unionInputGroupSearch(self, mixin):
        """
        Test the unionInputGroupSearch method to ensure correct internal calls
        and behavior for adding groups by text search.
        """
        db = "test_db"
        texts = [("text", "extra")]

        # Mock `generateGroupIDsBySearch` to return results, triggering loop
        mixin._loki.generateGroupIDsBySearch.return_value = [(1, 2, 3)]
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        # Simula `executemany` para retornar dados, ativando o loop
        mixin._loki._biofilter.db.cursor().executemany.return_value = [(1,)]

        # Call the method under test
        mixin.unionInputGroupSearch(db, texts)

        # Verify internal calls
        mixin.prepareTableForUpdate.assert_called_once_with(db, "group")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db group filter by text search ...\n"
        )
        mixin.logPop.assert_called_once_with("... OK: added 1 groups\n")
        assert mixin._inputFilters[db]["group"] == 1

    def test_intersectInputGroupSearch(self, mixin):
        """
        Test intersectInputGroupSearch to ensure it reduces the group filter
        when the group filter is already initialized.
        """
        db = "test_db"
        texts = [("text", "extra")]

        # Start group filter at 1 to trigger loop
        mixin._inputFilters[db]["group"] = 1

        # Mock to `generateGroupIDsBySearch`
        mixin._loki.generateGroupIDsBySearch.return_value = [(1, 2, 3)]
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin._loki._biofilter.db.cursor().getconnection().changes.side_effect = [10, 3]

        # Run method under test
        mixin.intersectInputGroupSearch(db, texts)

        # Checks
        mixin.prepareTableForQuery.assert_called_once_with(db, "group")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db group filter by text search ...\n"
        )
        mixin.logPop.assert_called_once_with(
            "... OK: kept 7 groups (3 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters[db]["group"] == 2

    def test_intersectInputGroupSearch_triggers_union(self, mixin):
        """
        Test intersectInputGroupSearch to ensure it calls unionInputGroupSearch
        when the group filter is not yet initialized.
        """
        db = "test_db"
        texts = [("text", "extra")]

        # Mock to `unionInputGroupSearch`
        mixin.unionInputGroupSearch = MagicMock()

        # Run method under test
        mixin.intersectInputGroupSearch(db, texts)

        # Chack if the `unionInputGroupSearch` method was called
        mixin.unionInputGroupSearch.assert_called_once_with(db, texts)
