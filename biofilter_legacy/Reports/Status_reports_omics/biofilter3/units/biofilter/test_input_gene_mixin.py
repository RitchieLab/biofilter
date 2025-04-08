import pytest
from unittest.mock import MagicMock, call
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
            tally["many"] = 1
            tally["zero"] = 1
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

        # Simula `executemany` para retornar dados, ativando o loop
        mixin._loki._biofilter.db.cursor().executemany.return_value = [(1,)]

        # Call the unionInputGenes method
        mixin.unionInputGenes(db, gene_list)

        # Verify internal calls
        mixin.prepareTableForUpdate.assert_called_once_with(db, "gene")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db gene filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 genes\n")
        assert mixin._inputFilters[db]["gene"] == 1

    def test_unionInputGenes_with_ambiguous_genes(self, mixin):
        """
        Test unionInputGenes when there are ambiguous genes and
        allow_ambiguous_genes is "yes".
        """
        db = "test_db"
        gene_list = [("namespace", "name", "extra")]
        mixin._options.allow_ambiguous_genes = "yes"  # Ambig Genes Allowed

        # Define `generateTypedBiopolymerIDsByIdentifiers` tally to "many"
        def generate_genes_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["match"] = 1
            tally["merge"] = 1
            tally["many"] = 2  # Define multi ambig results
            tally["zero"] = 0
            return [(1, 2, 3, 4)]

        # Mock `generateTypedBiopolymerIDsByIdentifiers` with tally for "many"
        mixin._loki.generateTypedBiopolymerIDsByIdentifiers = MagicMock(
            side_effect=generate_genes_with_tally
        )
        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        # Simulations the return of data to activate the loop
        mixin._loki._biofilter.db.cursor().executemany.return_value = [(1,)]

        # Call the unionInputGenes method
        mixin.unionInputGenes(db, gene_list)

        # Check internal calls
        mixin.prepareTableForUpdate.assert_called_once_with(db, "gene")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db gene filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 genes\n")
        assert mixin._inputFilters[db]["gene"] == 1
        # Check that the `warn` method was called with the ambiguity message
        mixin.warn.assert_called_once_with(
            "WARNING: added multiple results for 2 ambiguous gene identifier(s)\n"  # noqa E501
        )

    def test_intersectInputGenes(self, mixin):
        mixin._options.allow_ambiguous_genes = "no"

        # Define `generateTypedBiopolymerIDsByIdentifiers`
        def generate_genes_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["match"] = 1
            tally["merge"] = 1
            tally["many"] = 1
            tally["zero"] = 1
            return [(1, 2, 3, 4)]

        # Mock `generateTypedBiopolymerIDsByIdentifiers`
        mixin._loki.generateTypedBiopolymerIDsByIdentifiers = MagicMock(
            side_effect=generate_genes_with_tally
        )

        mixin._loki.generateTypedBiopolymerIDsByIdentifiers.return_value = MagicMock(
            side_effect=generate_genes_with_tally
        )  # noqa E501  # noqa E501  # noqa E501

        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin._inputFilters["test_db"]["gene"] = 1
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 1

        mixin.intersectInputGenes("test_db", [("namespace", "name")])

        mixin.prepareTableForQuery.assert_called_once_with("test_db", "gene")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db gene filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 genes (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters["test_db"]["gene"] == 2

    def test_intersectInputGenes_with_ambiguous_genes(self, mixin):
        mixin._options.allow_ambiguous_genes = "yes"

        # Define `generateTypedBiopolymerIDsByIdentifiers`
        def generate_genes_with_tally(*args, **kwargs):
            tally = kwargs.get("tally")
            tally["match"] = 1
            tally["merge"] = 1
            tally["many"] = 1
            tally["zero"] = 1
            return [(1, 2, 3, 4)]

        # Mock `generateTypedBiopolymerIDsByIdentifiers`
        mixin._loki.generateTypedBiopolymerIDsByIdentifiers = MagicMock(
            side_effect=generate_genes_with_tally
        )

        mixin._loki.generateTypedBiopolymerIDsByIdentifiers.return_value = MagicMock(
            side_effect=generate_genes_with_tally
        )  # noqa E501  # noqa E501  # noqa E501

        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin._inputFilters["test_db"]["gene"] = 1
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 1

        mixin.intersectInputGenes("test_db", [("namespace", "name")])

        mixin.prepareTableForQuery.assert_called_once_with("test_db", "gene")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.warn.assert_has_calls(
            [
                call("WARNING: ignored 1 unrecognized gene identifier(s)\n"),
                call(
                    "WARNING: kept multiple results for 1 ambiguous gene identifier(s)\n"  # noqa E501
                ),
            ],
            any_order=False,
        )

    def test_intersectInputGenes_triggers_union(self, mixin):
        """
        Test intersectInputGenes when _inputFilters[db]["gene"] is 0 to ensure
        it calls unionInputGenes instead of proceeding with intersection logic.
        """
        db = "test_db"
        names = [("namespace", "name")]
        errorCallback = MagicMock()

        # Set _inputFilters[db]["gene"] to 0 to trigger unionInputGenes
        mixin._inputFilters[db]["gene"] = 0

        # Mock unionInputGenes to verify it gets called
        mixin.unionInputGenes = MagicMock()

        # Call intersectInputGenes, which should call unionInputGenes
        mixin.intersectInputGenes(db, names, errorCallback)

        # Verify unionInputGenes was called with the correct arguments
        mixin.unionInputGenes.assert_called_once_with(db, names, errorCallback)

    def test_unionInputGeneSearch(self, mixin):
        mixin._loki.generateTypedBiopolymerIDsBySearch.return_value = [
            (1, 2, 3)
        ]  # noqa E501
        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        # Simula `executemany` para retornar dados, ativando o loop
        mixin._loki._biofilter.db.cursor().executemany.return_value = [(1,)]

        mixin.unionInputGeneSearch("test_db", [("text", "extra")])

        mixin.prepareTableForUpdate.assert_called_once_with("test_db", "gene")

        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "adding to test_db gene filter by text search ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 genes\n")
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
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 1

        mixin.intersectInputGeneSearch("test_db", [("text", "extra")])

        mixin.prepareTableForQuery.assert_called_once_with("test_db", "gene")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db gene filter by text search ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 genes (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters["test_db"]["gene"] == 2

    def test_intersectInputGeneSearch_triggers_union(self, mixin):
        """
        Test intersectInputGenes when _inputFilters[db]["gene"] is 0 to ensure
        it calls unionInputGenes instead of proceeding with intersection logic.
        """
        db = "test_db"
        names = [("text", "extra")]

        mixin._loki.generateTypedBiopolymerIDsBySearch.return_value = [
            (1, 2, 3)
        ]  # noqa E501
        mixin.getOptionTypeID = MagicMock(return_value=1)
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        mixin._inputFilters["test_db"]["gene"] = 1
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 1

        # Set _inputFilters[db]["gene"] to 0 to trigger unionInputGenes
        mixin._inputFilters[db]["gene"] = 0

        # Mock unionInputGenes to verify it gets called
        mixin.unionInputGeneSearch = MagicMock()

        mixin.intersectInputGeneSearch(db, names)

        # Verify unionInputGenes was called with the correct arguments
        mixin.unionInputGeneSearch.assert_called_once_with(db, names)
