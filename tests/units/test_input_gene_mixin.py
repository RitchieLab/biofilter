import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.input_gene_mixin import GeneInputMixin


class TestGeneInputMixin:
    @pytest.fixture
    def mixin(self):
        mixin = GeneInputMixin()
        mixin._loki = MagicMock()
        mixin._options = MagicMock()
        mixin._inputFilters = {"test_db": {"gene": 0}}
        return mixin

    def test_unionInputGenes(self, mixin):
        """
        Test the unionInputGenes method to ensure correct internal calls and
        tally handling.
        """
        db = "test_db"
        gene_list = [("namespace", "name", "extra")]
        mixin._options.allow_ambiguous_genes = "no"

        # Define `generateTypedBiopolymerIDsByIdentifiers`
        def generate_genes_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["match"] = 1
            tally["merge"] = 1
            tally["many"] = 0
            tally["zero"] = 0
            return [(1, 2, 3, 4)]

        # Mock `generateTypedBiopolymerIDsByIdentifiers`
        mixin._loki.generateTypedBiopolymerIDsByIdentifiers = MagicMock(
            side_effect=generate_genes_with_tally
        )
        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        # Call the unionInputGenes method
        mixin.unionInputGenes(db, gene_list)

        # Verify internal calls
        mixin.prepareTableForUpdate.assert_called_once_with(db, "gene")
        mixin._loki._db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db gene filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 0 genes\n")
        assert mixin._inputFilters[db]["gene"] == 1

    def test_intersectInputGenes(self, mixin):
        mixin._options.allow_ambiguous_genes = "no"

        # Define `generateTypedBiopolymerIDsByIdentifiers`
        def generate_genes_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["match"] = 1
            tally["merge"] = 1
            tally["many"] = 0
            tally["zero"] = 0
            return [(1, 2, 3, 4)]

        # Mock `generateTypedBiopolymerIDsByIdentifiers`
        mixin._loki.generateTypedBiopolymerIDsByIdentifiers = MagicMock(
            side_effect=generate_genes_with_tally
        )

        mixin._loki.generateTypedBiopolymerIDsByIdentifiers.return_value = MagicMock(
            side_effect=generate_genes_with_tally
        )  # noqa E501

        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin._inputFilters["test_db"]["gene"] = 1
        mixin._loki._db.cursor().getconnection().changes.return_value = 1

        mixin.intersectInputGenes("test_db", [("namespace", "name")])

        mixin.prepareTableForQuery.assert_called_once_with("test_db", "gene")
        mixin._loki._db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db gene filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 genes (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters["test_db"]["gene"] == 2

    def test_unionInputGeneSearch(self, mixin):
        mixin._loki.generateTypedBiopolymerIDsBySearch.return_value = [
            (1, 2, 3)
        ]  # noqa E501
        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        mixin.unionInputGeneSearch("test_db", [("text", "extra")])

        mixin.prepareTableForUpdate.assert_called_once_with("test_db", "gene")
        mixin._loki._db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db gene filter by text search ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 0 genes\n")
        assert mixin._inputFilters["test_db"]["gene"] == 1

    def test_intersectInputGeneSearch(self, mixin):
        mixin._loki.generateTypedBiopolymerIDsBySearch.return_value = [
            (1, 2, 3)
        ]  # noqa E501
        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        mixin._inputFilters["test_db"]["gene"] = 1
        mixin._loki._db.cursor().getconnection().changes.return_value = 1

        mixin.intersectInputGeneSearch("test_db", [("text", "extra")])

        mixin.prepareTableForQuery.assert_called_once_with("test_db", "gene")
        mixin._loki._db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db gene filter by text search ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 genes (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters["test_db"]["gene"] == 2
