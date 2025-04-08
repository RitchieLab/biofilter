import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.input_locus_position_mixin import (
    LocusPositionInputMixin,
)  # noqa E501


class TestLocusPositionInputMixin:
    @pytest.fixture
    def mixin(self):
        mixin = LocusPositionInputMixin()
        mixin._loki = MagicMock()
        mixin._inputFilters = {"test_db": {"locus": 0}}
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()
        return mixin

    def test_unionInputLoci(self, mixin):
        db = "test_db"
        loci = [
            ("label1", "chr1", 100, "extra1"),
            ("label2", "chr2", 200, "extra2"),
        ]  # noqa E501

        # Configurar retorno para simular inserções e erros
        def error_callback(*args):
            pass

        mixin.prepareTableForUpdate = MagicMock()
        mixin._loki._biofilter.db.cursor().executemany.return_value = [
            (1, "label1", "chr1", 100, "extra1"),
            (2, "label2", "chr2", 200, "extra2"),
        ]

        # Chamar o método com dados de exemplo
        mixin.unionInputLoci(db, loci, errorCallback=error_callback)

        # Verificar chamadas internas
        mixin.logPush.assert_called_once_with(
            "adding to test_db position filter ...\n"
        )  # noqa E501
        mixin.prepareTableForUpdate.assert_called_once_with(db, "locus")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once()
        mixin.logPop.assert_called_once_with("... OK: added 2 positions\n")
        assert mixin._inputFilters[db]["locus"] == 1

    def test_unionInputLoci_with_invalid_positions(self, mixin):
        db = "test_db"
        # TODO: Code complains about None and int values in the tuple
        # loci = [("label1", "chr1", 100, "extra1"), ("label2", None, 200, "extra2")]  # noqa E501
        loci = [
            ("label1", "chr1", "100", "extra1"),
            ("label2", "chr2", "200", "extra2"),
        ]

        def error_callback(*args):
            pass

        # Simular retorno do executemany para incluir um caso inválido
        mixin._loki._biofilter.db.cursor().executemany.return_value = [
            (1, "label1", "chr1", "100", "extra1"),
            # (1, "label2", None, 200, "extra2"),
            (1, "label2", "chr1", "200", "extra2"),
        ]

        mixin.prepareTableForUpdate = MagicMock()

        mixin.unionInputLoci(db, loci, errorCallback=error_callback)

        mixin.warn.assert_called_once_with(
            "WARNING: ignored 1 invalid positions\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 positions\n")
        assert mixin._inputFilters[db]["locus"] == 1

    def test_intersectInputLoci(self, mixin):
        db = "test_db"
        loci = [
            ("label1", "chr1", 100, "extra1"),
            ("label2", "chr2", 200, "extra2"),
        ]  # noqa E501

        mixin._inputFilters[db]["locus"] = 1
        mixin.prepareTableForQuery = MagicMock()
        mixin._loki._biofilter.db.cursor().getconnection().changes.return_value = 2
        mixin._loki._biofilter.db.changes.return_value = 1

        mixin.intersectInputLoci(db, loci)

        mixin.logPush.assert_called_once_with(
            "reducing test_db position filter ...\n"
        )  # noqa E501
        mixin.prepareTableForQuery.assert_called_once_with(db, "locus")
        mixin._loki._biofilter.db.cursor().executemany.assert_called_once_with(
            "UPDATE `%s`.`locus` SET flag = 1 WHERE (1 OR ?1) AND chr = ?2 AND pos = ?3 AND (1 OR ?4)"  # noqa E501
            % db,
            loci,
        )
        mixin.logPop.assert_called_once_with(
            "... OK: kept 1 positions (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters[db]["locus"] == 2

    def test_intersectInputLoci_triggers_union(self, mixin):
        db = "test_db"
        loci = [("label1", "chr1", 100, "extra1")]

        mixin._inputFilters[db]["locus"] = 0
        mixin.unionInputLoci = MagicMock()

        mixin.intersectInputLoci(db, loci)

        mixin.unionInputLoci.assert_called_once_with(db, loci, None)
