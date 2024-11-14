import pytest
from unittest.mock import MagicMock, call
from biofilter_modules.mixins.input_snp_mixin import SNPInputMixin


@pytest.fixture
def snp_input_mixin():
    """
    Fixture that creates an instance of SNPInputMixin with mocks for the
    database and other dependencies. It serves to isolate the unionInputSNPs
    method.
    """
    mixin = SNPInputMixin()
    # Replace _loki._db with a MagicMock to simulate database interactions
    mixin._loki = MagicMock()
    mixin._loki._db.cursor.return_value = MagicMock()
    # Setting the inputFilters to track state changes
    mixin._inputFilters = {"test_db": {"snp": 0}}
    return mixin


def test_unionInputSNPs(snp_input_mixin):
    """
    Test the unionInputSNPs method to ensure that internal functions are called
    correctly and that the expected behavior occurs without accessing a real
    database.
    """
    db = "test_db"
    snps = [("rs1", "extra1"), ("rs2", "extra2")]
    errorCallback = MagicMock()

    # Define a return value to simulate `generateMergedFilteredSNPs`
    def generate_snps_with_tally(snps, tally, errorCallback):
        tally["match"] = 1
        tally["merge"] = 1
        tally["many"] = 0
        return snps

    # Mock the methods called by unionInputSNPs
    snp_input_mixin.generateMergedFilteredSNPs = MagicMock(
        side_effect=generate_snps_with_tally
    )
    snp_input_mixin.logPush = MagicMock()
    snp_input_mixin.logPop = MagicMock()
    snp_input_mixin.prepareTableForUpdate = MagicMock()

    # Call the method under test
    snp_input_mixin.unionInputSNPs(db, snps, errorCallback)

    # Verify that the expected methods were called
    snp_input_mixin.logPush.assert_called_once_with(
        "adding to %s SNP filter ...\n" % db
    )
    # Verify that the prepareTableForUpdate method was called w/correct args
    snp_input_mixin.prepareTableForUpdate.assert_called_once_with(db, "snp")
    # Verify that the executemany method was called w/correct SQL and SNPs
    snp_input_mixin._loki._db.cursor().executemany.assert_called_once()
    snp_input_mixin.logPop.assert_called_once()
    assert snp_input_mixin._inputFilters[db]["snp"] == 1


def test_unionInputSNPs_with_ambiguous_snps(snp_input_mixin):
    """
    Test the unionInputSNPs method to ensure it handles ambiguous SNPs
    correctly.
    """
    db = "test_db"
    snps = [("rs1", "extra1"), ("rs2", "extra2")]
    errorCallback = MagicMock()

    # Define a return value to simulate `generateMergedFilteredSNPs`
    def generate_snps_with_tally(snps, tally, errorCallback):
        tally["match"] = 1
        tally["merge"] = 1
        tally["many"] = 1
        return snps

    # Mock the methods called by unionInputSNPs
    snp_input_mixin.generateMergedFilteredSNPs = MagicMock(
        side_effect=generate_snps_with_tally
    )
    snp_input_mixin.logPush = MagicMock()
    snp_input_mixin.logPop = MagicMock()
    snp_input_mixin.prepareTableForUpdate = MagicMock()

    # Call the method under test
    snp_input_mixin.unionInputSNPs(db, snps, errorCallback)

    # Verify that the expected methods were called
    snp_input_mixin.logPush.assert_called_once_with(
        "adding to %s SNP filter ...\n" % db
    )
    # Verify that the prepareTableForUpdate method was called w/correct args
    snp_input_mixin.prepareTableForUpdate.assert_called_once_with(db, "snp")
    # Verify that the executemany method was called w/correct SQL and SNPs
    snp_input_mixin._loki._db.cursor().executemany.assert_called_once()
    snp_input_mixin.logPop.assert_called_once_with(
        "... OK: added 1 SNPs (1 RS#s merged, 1 ambiguous)\n"
    )
    assert snp_input_mixin._inputFilters[db]["snp"] == 1


def test_intersectInputSNPs_initializes_with_union(snp_input_mixin):
    db = "test_db"
    snps = [("rs1", "extra1"), ("rs2", "extra2")]
    errorCallback = MagicMock()

    snp_input_mixin.unionInputSNPs = MagicMock()

    snp_input_mixin.intersectInputSNPs(db, snps, errorCallback)

    snp_input_mixin.unionInputSNPs.assert_called_once_with(
        db, snps, errorCallback
    )  # noqa  E501


def test_intersectInputSNPs(snp_input_mixin):
    """
    Test the intersectInputSNPs method to ensure that internal functions are
    called correctly and that the expected behavior occurs without accessing a
    real database.
    """
    db = "test_db"
    snps = [("rs1", "extra1"), ("rs2", "extra2")]
    errorCallback = MagicMock()

    # Start the SNP filter for the database
    snp_input_mixin._inputFilters[db]["snp"] = 1  # Start the SNP filter
    snp_input_mixin.logPush = MagicMock()
    snp_input_mixin.logPop = MagicMock()
    snp_input_mixin.prepareTableForQuery = MagicMock()

    # Define a return value to simulate `generateCurrentRSesByRSes` with the
    # tally filled
    def generate_snps_with_tally(snps, tally):
        tally["match"] = 1
        tally["merge"] = 1
        tally["many"] = 0
        return snps

    snp_input_mixin._loki.generateCurrentRSesByRSes = MagicMock(
        side_effect=generate_snps_with_tally
    )
    snp_input_mixin._loki._db.cursor().getconnection.return_value.changes.return_value = (  # noqa  E501
        10  # Simula 10 changes
    )

    # Run the method
    snp_input_mixin.intersectInputSNPs(db, snps, errorCallback)

    # Check if the logPush method was called with the correct message
    snp_input_mixin.logPush.assert_called_once_with(
        "reducing %s SNP filter ...\n" % db
    )  # noqa  E501
    snp_input_mixin.prepareTableForQuery.assert_called_once_with(db, "snp")
    snp_input_mixin._loki._db.cursor().execute.assert_has_calls(
        [
            call("UPDATE `%s`.`snp` SET flag = 0" % db),
            call("DELETE FROM `%s`.`snp` WHERE flag = 0" % db),
        ]
    )
    snp_input_mixin._loki._db.cursor().executemany.assert_called_once_with(
        "UPDATE `%s`.`snp` SET flag = 1 WHERE (1 OR ?1 OR ?2) AND rs = ?3"
        % db,  # noqa  E501
        snps,  # noqa  E501
    )
    snp_input_mixin.logPop.assert_called_once_with(
        "... OK: kept 0 SNPs (10 dropped, 1 RS#s merged)\n"
    )
    assert snp_input_mixin._inputFilters[db]["snp"] == 2
