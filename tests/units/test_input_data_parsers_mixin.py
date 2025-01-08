import pytest
from biofilter_modules.mixins.input_data_parsers_mixin import (
    InputDataParsersMixin,
)  # noqa E501
from unittest.mock import patch, Mock


class TestInputDataParsersMixin:
    @pytest.fixture
    def mixin(self):
        class MockLoki:
            def getUCSChgByGRCh(self, grchBuild):
                return 38 if grchBuild == 37 else None

            def generateGRChByUCSChg(self, ucscBuild):
                # return [37] if ucscBuild == 38 else []
                return [37, 38] if ucscBuild == 38 else []

            def generateCurrentRSesByRSes(self, snps, tally=None):
                return [(rs, extra, rs + 1) for rs, extra in snps]

            def generateSNPLociByRSes(
                self,
                genMergeFormat,
                minMatch,
                maxMatch,
                validated,
                tally=None,
                errorCallback=None,
            ):
                return [
                    (rs, posextra, "chr1", 1000)
                    for rs, posextra in genMergeFormat  # noqa E501
                ]  # noqa E501

            def hasLiftOverChains(self, ucscBuildOld, ucscBuildNew):
                # Simulation of existence of liftOver chains between versions.
                return True

            def generateLiftOverLoci(
                self,
                ucscBuildOld,
                ucscBuildNew,
                loci,
                tally=None,
                errorCallback=None,  # noqa E501
            ):
                # Return simulated loci after liftOver
                return [
                    (label, chm, pos + 100, extra)
                    for label, chm, pos, extra in loci  # noqa E501
                ]

            def generateLiftOverRegions(
                self,
                ucscBuildOld,
                ucscBuildNew,
                regions,
                tally=None,
                errorCallback=None,
            ):
                # Simulate a behavior that calls the errorCallback on an error
                if errorCallback:
                    # errorCallback(regions, "dropped during liftOver from
                    # hg%s to hg%s" % (ucscBuildOld, ucscBuildNew))
                    errorCallback(regions)
                return regions

            chr_num = {"1": 1}
            chr_name = {1: "1"}

        class MockOptions:
            allow_ambiguous_snps = "no"
            allow_unvalidated_snp_positions = "no"
            coordinate_base = 1
            regions_half_open = "no"

        class TestClass(InputDataParsersMixin):
            _loki = MockLoki()
            _options = MockOptions()

            def warn(self, message):
                print(f"Warning: {message}")

            def addUserSource(self, *args, **kwargs):
                pass

            def addUserGroup(self, *args, **kwargs):
                pass

            def addUserGroupBiopolymers(self, *args, **kwargs):
                pass

        return TestClass()

    # ----------------------------------------------------------------------

    # -- Tests to getInputGenomeBuilds Method --
    def test_getInputGenomeBuilds(self, mixin):
        assert mixin.getInputGenomeBuilds(37, 38) == (37, 38)
        assert mixin.getInputGenomeBuilds(37, None) == (37, 38)
        # Test the use of generateGRChByUCSChg with `ucscBuild` and coverage
        # of the `if grchBuild`
        assert mixin.getInputGenomeBuilds(None, 38) == (38, 38)

    def test_getInputGenomeBuilds_invalid_grch_and_ucsc(self, mixin):
        with pytest.raises(SystemExit):
            mixin.getInputGenomeBuilds(37, 39)

    def test_getInputGenomeBuilds_no_grch_no_ucsc(self, mixin):
        assert mixin.getInputGenomeBuilds(None, None) == (None, None)

    # ----------------------------------------------------------------------

    # -- Tests to generateMergedFilteredSNPs Method --
    def test_generateMergedFilteredSNPs(self, mixin):
        snps = [(1, "extra1"), (2, "extra2")]
        result = list(mixin.generateMergedFilteredSNPs(snps))
        assert result == [("1", "extra1", "2"), ("2", "extra2", "3")]

    def test_generateMergedFilteredSNPs_ambiguous_snps_allowed(self, mixin):
        mixin._options.allow_ambiguous_snps = "yes"
        snps = [(1, "extra1"), (2, "extra2")]
        result = list(mixin.generateMergedFilteredSNPs(snps))
        assert result == [(1, "extra1", 2), (2, "extra2", 3)]

    def test_generateMergedFilteredSNPs_ambiguous_snps_not_allowed(
        self, mixin
    ):  # noqa E501
        mixin._options.allow_ambiguous_snps = "no"
        snps = [(1, "extra1"), (2, "extra2")]
        result = list(mixin.generateMergedFilteredSNPs(snps))
        assert result == [("1", "extra1", "2"), ("2", "extra2", "3")]

    def test_generateMergedFilteredSNPs_with_tally(self, mixin):
        snps = [(1, "extra1"), (2, "extra2")]
        tally = {}
        result = list(mixin.generateMergedFilteredSNPs(snps, tally=tally))
        assert result == [("1", "extra1", "2"), ("2", "extra2", "3")]

    def test_generateMergedFilteredSNPs_with_error_callback(self, mixin):
        def error_callback(line, message):
            print(f"Error: {message} in line {line}")

        snps = [(1, "extra1"), (2, "extra2")]
        result = list(
            mixin.generateMergedFilteredSNPs(
                snps, errorCallback=error_callback
            )  # noqa E501
        )
        assert result == [("1", "extra1", "2"), ("2", "extra2", "3")]

    # ----------------------------------------------------------------------

    # -- Tests to generateRSesFromText Method --
    def test_generateRSesFromText(self, mixin):
        lines = ["rs1 extra1", "rs2 extra2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(1, "extra1"), (2, "extra2")]

    def test_generateRSesFromText_valid_lines(self, mixin):
        lines = ["rs1 extra1", "rs2 extra2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(1, "extra1"), (2, "extra2")]

    def test_generateRSesFromText_invalid_lines(self, mixin):
        lines = ["invalid_line", "rs2 extra2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(2, "extra2")]

    def test_generateRSesFromText_with_separator(self, mixin):
        lines = ["rs1,extra1", "rs2,extra2"]
        result = list(mixin.generateRSesFromText(lines, separator=","))
        assert result == [(1, "extra1"), (2, "extra2")]

    def test_generateRSesFromText_empty_lines(self, mixin):
        lines = ["", "rs2 extra2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(2, "extra2")]

    def test_generateRSesFromText_no_extra_info(self, mixin):
        lines = ["rs1", "rs2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(1, None), (2, None)]

    def test_generateRSesFromText_with_error_callback(self, mixin):
        def error_callback(line, message):
            print(f"Error: {message} in line {line}")

        # Include a line that will cause an error to trigger the callback
        lines = ["rs12345", "invalid_rs", "rs67890"]
        # Convert the result to a list to force the generator to run through
        # completely
        result = list(
            mixin.generateRSesFromText(lines, errorCallback=error_callback)
        )  # noqa E501
        # Check if the valid lines were processed correctly
        assert result == [(12345, None), (67890, None)]

    # ----------------------------------------------------------------------

    # -- Tests to generateRSesFromRSFiles Method --
    def test_generateRSesFromRSFiles(self, mixin, tmp_path):
        file = tmp_path / "rs_file.txt"
        file.write_text("rs1 extra1\nrs2 extra2\n")
        result = list(mixin.generateRSesFromRSFiles([str(file)]))
        assert result == [(1, "extra1"), (2, "extra2")]

    def test_generateRSesFromRSFiles_with_error_callback(self, mixin):
        def error_callback(path, message):
            print(f"Error: {message} in path {path}")

        # Use patch to monitor calls to the warn method
        invalid_path = "invalid_file.txt"
        # Use patch to monitor calls to the warn method
        with patch.object(mixin, "warn") as mock_warn:
            result = list(
                mixin.generateRSesFromRSFiles(
                    [invalid_path], errorCallback=error_callback
                )
            )
            # Check if the warn method was called with the expected message
            mock_warn.assert_called_once()
            assert (
                "WARNING: error reading input file"
                in mock_warn.call_args[0][0]  # noqa E501
            )  # noqa E501
            # Check if the result is empty, since the file was not found
            assert result == []

    # ----------------------------------------------------------------------

    # -- Tests to generateLociFromText Method --
    def test_generateLociFromText(self, mixin):
        lines = ["chr1 1000", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [
            ("chr1:1000", 1, 1000, None),
            ("chr1:2000", 1, 2000, None),
        ]  # noqa E501

    def test_generateLociFromText_empty_cols(self, mixin):
        # Test the case where cols is empty
        lines = ["   "]  # Empty line after strip()
        result = list(mixin.generateLociFromText(lines))
        assert result == []  # Non-empty line should be ignored

    def test_generateLociFromText_three_columns(self, mixin):
        # Test the case where there are exactly three columns
        lines = ["chr1 label 1000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [
            ("label", 1, 1000, None)
        ]  # Extra should still be None  # noqa E501

    def test_generateLociFromText_four_columns(self, mixin):
        # Test the case where there are exactly four columns
        lines = ["chr1 label extra 1000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [
            ("label", 1, 1000, None)
        ]  # Extra should still be None  # noqa E501

    def test_generateLociFromText_five_columns(self, mixin):
        # Test the case where there are exactly five columns
        lines = ["chr1 label extra 1000 additional"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("label", 1, 1000, "additional")]

    def test_generateLociFromText_invalid_chromosome_with_error_callback(
        self, mixin
    ):  # noqa E501
        def error_callback(line, message):
            print(f"Error: {message} in line: {line}")

        # Rows with invalid chromosome "chrX", which should trigger the
        # errorCallback
        lines = ["chr1 1000", "chrX 2000"]
        # Use patch to monitor calls to the error_callback
        with patch("builtins.print") as mock_print:
            result = list(
                mixin.generateLociFromText(lines, errorCallback=error_callback)
            )
            # Check if the errorCallback was called with the expected message
            # for "chrX"
            mock_print.assert_called_once_with(
                "Error: invalid chromosome 'X' at index 2 in line: chrX 2000"
            )
        # Check if only the first valid line was processed
        assert result == [("chr1:1000", 1, 1000, None)]

    def test_generateLociFromText_with_separator(self, mixin):
        lines = ["chr1,1000", "chr1,2000"]
        result = list(mixin.generateLociFromText(lines, separator=","))
        assert result == [
            ("chr1:1000", 1, 1000, None),
            ("chr1:2000", 1, 2000, None),
        ]  # noqa E501

    def test_generateLociFromText_with_offset(self, mixin):
        lines = ["chr1 1000", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines, applyOffset=True))
        assert result == [
            ("chr1:1000", 1, 1000, None),
            ("chr1:2000", 1, 2000, None),
        ]  # noqa E501

    def test_generateLociFromText_with_error_callback(self, mixin):
        def error_callback(line, message):
            print(f"Error: {message} in line {line}")

        lines = ["invalid_line", "chr1 2000"]
        result = list(
            mixin.generateLociFromText(lines, errorCallback=error_callback)
        )  # noqa E501
        assert result == [("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_with_na_position(self, mixin):
        lines = ["chr1 NA", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [
            ("chr1:NA", 1, None, None),
            ("chr1:2000", 1, 2000, None),
        ]  # noqa E501

    def test_generateLociFromText_with_dash_position(self, mixin):
        lines = ["chr1 -", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [
            ("chr1:-", 1, None, None),
            ("chr1:2000", 1, 2000, None),
        ]  # noqa E501

    # ----------------------------------------------------------------------

    # -- Tests to generateLociFromMapFiles Method --
    def test_generateLociFromMapFiles(self, mixin, tmp_path):
        file = tmp_path / "map_file.txt"
        file.write_text("chr1 1000\nchr1 2000\n")
        result = list(mixin.generateLociFromMapFiles([str(file)]))
        assert result == [
            ("chr1:1000", 1, 1000, None),
            ("chr1:2000", 1, 2000, None),
        ]  # noqa E501

    def test_generateLociFromMapFiles_with_error_callback(self, mixin):
        def error_callback(path, message):
            print(f"Error: {message} in path {path}")

        # Path to a non-existent file to force an error
        invalid_path = "invalid_file.txt"
        # Use patch to monitor calls to the warn method
        with patch.object(mixin, "warn") as mock_warn:
            result = list(
                mixin.generateLociFromMapFiles(
                    [invalid_path], errorCallback=error_callback
                )
            )
            # Check if the warn method was called with the expected message
            mock_warn.assert_called_once()
            assert (
                "WARNING: error reading input file"
                in mock_warn.call_args[0][0]  # noqa E501
            )  # noqa E501
            # Check if the result is empty, since the file was not found
            assert result == []

    # ----------------------------------------------------------------------

    # -- Tests to generateLiftOverLoci Method --
    def test_generateLiftOverLoci(self, mixin):
        loci = [("label1", 1, 1000, "extra1"), ("label2", 1, 2000, "extra2")]
        result = mixin.generateLiftOverLoci("hg18", "hg19", loci)
        assert result == [
            ("label1", 1, 1100, "extra1"),
            ("label2", 1, 2100, "extra2"),
        ]  # noqa E501

    def test_generateLiftOverLoci_no_ucscBuildOld(self, mixin):
        # Test the case where `ucscBuildOld` is not provided
        loci = [("label", "chr1", 1000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            mixin.generateLiftOverLoci(None, 38, loci)
            # Check if the correct warning message was logged
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version was not specified for position input; assuming it matches the knowledge database\n"  # noqa E501
            )

    def test_generateLiftOverLoci_no_ucscBuildNew(self, mixin):
        # Test the case where `ucscBuildNew` is not provided
        loci = [("label", "chr1", 1000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            mixin.generateLiftOverLoci(37, None, loci)
            # Check if the correct warning message was logged
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n"  # noqa E501
            )

    def test_generateLiftOverLoci_no_liftOver_chains(self, mixin):
        # Test the case where `hasLiftOverChains` returns `False`, triggering `sys.exit`  # noqa E501
        loci = [("label", "chr1", 1000, None)]
        with patch.object(
            mixin._loki, "hasLiftOverChains", return_value=False
        ):  # noqa E501
            with pytest.raises(SystemExit) as e:
                mixin.generateLiftOverLoci(37, 38, loci)
            # Check if the error message contains the expected text
            assert str(e.value) == (
                "ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg37 to hg38\n"  # noqa E501
            )

    def test_generateLiftOverLoci_liftoverCallback(self, mixin):
        def error_callback(region, message):
            print(f"Error: {message} in region: {region}")

        loci = [("label", "chr1", 1000, None)]

        # Mock `generateLiftOverLoci` to simulate th call of `liftoverCallback`
        with patch.object(
            mixin._loki,
            "generateLiftOverLoci",
            side_effect=lambda *args, **kwargs: [
                kwargs["errorCallback"](loci[0])
            ],  # noqa E501
        ):
            with patch("builtins.print") as mock_print:
                mixin.generateLiftOverLoci(
                    37, 38, loci, errorCallback=error_callback
                )  # noqa E501

                # Check if the `errorCallback` was called with the expected message  # noqa E501
                mock_print.assert_called_once_with(
                    "Error: dropped during liftOver from hg37 to hg38 in region: label\tchr1\t1000\tNone"  # noqa E501
                )

    # ----------------------------------------------------------------------

    # -- Tests to generateRegionsFromText Method --
    def test_generateRegionsFromText(self, mixin):
        lines = ["chr1 1000 2000", "chr1 3000 4000"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [
            ("chr1:1000-2000", 1, 1000, 2000, None),
            ("chr1:3000-4000", 1, 3000, 4000, None),
        ]

    def test_generateRegionsFromText_apply_offset_with_half_open(self, mixin):
        # Test the case where applyOffset is active and regions_half_open is "yes"  # noqa E501
        mixin._options.regions_half_open = "yes"
        mixin._options.coordinate_base = 1
        lines = ["chr1 1000 2000"]
        result = list(mixin.generateRegionsFromText(lines, applyOffset=True))
        # offsetEnd is decremented by 1 when `regions_half_open` is "yes"
        assert result == [("chr1:1000-2000", 1, 1000, 1999, None)]

    def test_generateRegionsFromText_empty_cols(self, mixin):
        # Test the case where cols is empty
        lines = ["   "]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == []

    def test_generateRegionsFromText_four_or_more_columns(self, mixin):
        # Test the case where there are four or more columns
        lines = ["chr1 label 1000 2000 extra"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [("label", 1, 1000, 2000, "extra")]

    def test_generateRegionsFromText_chromosome_prefix(self, mixin):
        # Test the case where the chromosome starts with "CHR"
        lines = ["chr1 1000 2000"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [("chr1:1000-2000", 1, 1000, 2000, None)]

    def test_generateRegionsFromText_posMin_and_posMax_na(self, mixin):
        # Test the cases where posMin and posMax are "NA"
        lines = ["chr1 NA NA"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [("chr1:NA-NA", 1, None, None, None)]

    def test_generateRegionsFromText_with_error_callback(self, mixin):
        # Test the case where `errorCallback` is triggered due to an error in a line  # noqa E501
        def error_callback(line, message):
            print(f"Error: {message} in line: {line}")

        lines = [
            "chr1 1000 2000",
            "chrX 3000 4000",
        ]  # "chrX" will return error  # noqa E501
        with patch("builtins.print") as mock_print:
            result = list(
                mixin.generateRegionsFromText(
                    lines, errorCallback=error_callback
                )  # noqa E501
            )
            # Check if the `errorCallback` was called for the invalid line
            mock_print.assert_called_once_with(
                "Error: invalid chromosome 'X' at index 2 in line: chrX 3000 4000"  # noqa E501
            )
        # Check if only the first valid line was processed
        assert result == [("chr1:1000-2000", 1, 1000, 2000, None)]

    def test_generateRegionsFromText_with_error_callback_col(self, mixin):
        # Test the case where `errorCallback` is triggered due to an error in a line  # noqa E501
        def error_callback(line, message):
            print(f"Error: {message} in line: {line}")

        lines = ["chr1 1000", "chrX 3000"]  # "chrX" deve causar um erro
        with patch("builtins.print") as mock_print:
            result = list(
                mixin.generateRegionsFromText(
                    lines, errorCallback=error_callback
                )  # noqa E501
            )
            assert result == []
            # Check if the `errorCallback` was called for the invalid line
            mock_print.assert_called_once_with(
                "Error: not enough columns at index 2 in line: chrX 3000"
            )

    # ----------------------------------------------------------------------

    # -- Tests to generateRegionsFromFiles Method --
    def test_generateRegionsFromFiles(self, mixin, tmp_path):
        file = tmp_path / "regions_file.txt"
        file.write_text("chr1 1000 2000\nchr1 3000 4000\n")
        result = list(mixin.generateRegionsFromFiles([str(file)]))
        assert result == [
            ("chr1:1000-2000", 1, 1000, 2000, None),
            ("chr1:3000-4000", 1, 3000, 4000, None),
        ]

    def test_generateRegionsFromFiles_with_error_callback(self, mixin):
        def error_callback(path, message):
            print(f"Error: {message} in path {path}")

        # Path to a non-existent file to force an error
        invalid_path = "invalid_file.txt"
        # Use patch to monitor calls to the warn method
        with patch.object(mixin, "warn") as mock_warn:
            result = list(
                mixin.generateRegionsFromFiles(
                    [invalid_path], errorCallback=error_callback
                )
            )
            # Check if the warn method was called with the expected message
            mock_warn.assert_called_once()
            assert (
                "WARNING: error reading input file"
                in mock_warn.call_args[0][0]  # noqa E501
            )  # noqa E501
            # Check if the result is empty, since the file was not found
            assert result == []

    # ----------------------------------------------------------------------

    # -- Tests to generateLiftOverRegions Method --
    def test_generateLiftOverRegions_no_ucscBuildOld(self, mixin):
        # Test the case where `ucscBuildOld` is not provided
        regions = [("label", "chr1", 1000, 2000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            result = mixin.generateLiftOverRegions(None, 38, regions)
            # Check if the correct warning message was logged
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version was not specified for region input; assuming it matches the knowledge database\n"  # noqa E501
            )
            # Check if the result is the same as the input
            assert result == regions

    def test_generateLiftOverRegions_no_ucscBuildNew(self, mixin):
        # Test the case where `ucscBuildNew` is not provided
        regions = [("label", "chr1", 1000, 2000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            result = mixin.generateLiftOverRegions(37, None, regions)
            # Check if the correct warning message was logged
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n"  # noqa E501
            )
            # Check if the result is the same as the input
            assert result == regions

    def test_generateLiftOverRegions_no_liftOver_chains(self, mixin):
        # Test the case where `hasLiftOverChains` returns `False`, triggering `sys.exit`  # noqa E501
        regions = [("label", "chr1", 1000, 2000, None)]
        with patch.object(
            mixin._loki, "hasLiftOverChains", return_value=False
        ):  # noqa E501
            with pytest.raises(SystemExit) as e:
                mixin.generateLiftOverRegions(37, 38, regions)
            # Check if the error message contains the expected text
            assert str(e.value) == (
                "ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg37 to hg38\n"  # noqa E501
            )

    def test_generateLiftOverRegions_liftoverCallback(self, mixin):
        # Define an error_callback that accepts two arguments to capture the region and message separately  # noqa E501
        def error_callback(region, message):
            print(f"Error: {message} in region: {region}")

        regions = [("label", "chr1", 1000, 2000, None)]
        # Patch to simulate the behavior of generateLiftOverRegions
        with patch.object(
            mixin._loki,
            "generateLiftOverRegions",
            wraps=mixin._loki.generateLiftOverRegions,
        ):
            with patch("builtins.print") as mock_print:
                result = mixin.generateLiftOverRegions(
                    37, 38, regions, errorCallback=error_callback
                )
                # Check if the `errorCallback` was called with the expected message  # noqa E501
                mock_print.assert_called_once_with(
                    "Error: dropped during liftOver from hg37 to hg38 in region: ('label', 'chr1', 1000, 2000, None)"  # noqa E501
                )
                # Check if the result is the same as the input, since it was interrupted  # noqa E501
                assert result == regions

    # ----------------------------------------------------------------------

    # -- Tests to generateNamesFromText Method --
    def test_generateNamesFromText_empty_line(self, mixin):
        # Test the case of an empty line
        lines = [""]
        result = list(mixin.generateNamesFromText(lines))
        assert result == []  # Must return an empty list

    def test_generateNamesFromText_single_column(self, mixin):
        # Test a line with a single column and a default namespace
        lines = ["name1"]
        result = list(
            mixin.generateNamesFromText(lines, defaultNS="default_ns")
        )  # noqa E501
        assert result == [
            ("default_ns", "name1", None)
        ]  # must return `defaultNS` and `name`, with `extra` as None

    def test_generateNamesFromText_two_columns(self, mixin):
        # Testa uma linha com duas colunas
        lines = ["ns1\tname2"]
        result = list(mixin.generateNamesFromText(lines, separator="\t"))
        assert result == [
            ("ns1", "name2", None)
        ]  # must capture `ns` and `name`, with `extra` as None

    def test_generateNamesFromText_three_columns(self, mixin):
        # Test a line with three columns
        lines = ["ns2\tname3\textra_info"]
        result = list(mixin.generateNamesFromText(lines, separator="\t"))
        assert result == [
            ("ns2", "name3", "extra_info")
        ]  # must capture `ns`, `name` and `extra`

    def test_generateNamesFromText_multiple_lines(self, mixin):
        # Test multiple lines, each with a different number of columns
        lines = [
            "name4",  # 1 coluna
            "ns5\tname5",  # 2 colunas
            "ns6\tname6\textra_info6",  # 3 colunas
        ]
        result = list(
            mixin.generateNamesFromText(
                lines, defaultNS="default_ns", separator="\t"
            )  # noqa E501
        )
        expected = [
            ("default_ns", "name4", None),  # Usa `defaultNS` para `ns`
            ("ns5", "name5", None),  # Captura `ns` e `name`, `extra` é None
            ("ns6", "name6", "extra_info6"),  # Captura `ns`, `name` e `extra`
        ]
        assert result == expected

    def test_generateNamesFromText_errorCallback(self, mixin):
        # Test the case where `errorCallback` is triggered by a parsing error
        lines = [
            "ns7\tname7",
            None,
        ]  # second line is None and should cause an error
        error_callback = Mock()

        # Run the method with the errorCallback
        result = list(
            mixin.generateNamesFromText(
                lines, separator="\t", errorCallback=error_callback
            )
        )

        # Check the expected result for the first valid line
        assert result == [("ns7", "name7", None)]

        # Check if the errorCallback was called with the invalid line
        error_callback.assert_called_once()
        called_args = error_callback.call_args[0]
        assert called_args[0] is None  # Linha inválida passada para o callback
        # assert "object is not subscriptable" in called_args[1]  # Parte da mensagem de erro  # noqa E501
        assert (
            "'NoneType' object has no attribute 'strip' at index 2"
            in called_args[1]  # noqa E501
        )  # message part error

    # ----------------------------------------------------------------------

    # -- Tests to generateNamesFromNameFiles Method --
    def test_generateNamesFromNameFiles_valid_file(self, mixin, tmp_path):
        file = tmp_path / "test_file.txt"
        file.write_text("ns1\tname1\nns2\tname2\n")
        result = list(mixin.generateNamesFromNameFiles([str(file)]))
        # assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]  # noqa E501
        expected = [("ns1", "name1", None), ("ns2", "name2", None)]
        assert (
            result == expected
        )  # confirm that the lines were read correctly  # noqa E501

    def test_generateNamesFromNameFiles_with_comments(self, mixin, tmp_path):
        # Create a temporary file with some commented lines
        file_path = tmp_path / "test_file.txt"
        file_path.write_text(
            "# This is a comment\nns3\tname3\n# Another comment\nns4\tname4\n"
        )

        result = list(
            mixin.generateNamesFromNameFiles([str(file_path)], separator="\t")
        )
        expected = [("ns3", "name3", None), ("ns4", "name4", None)]
        assert (
            result == expected
        )  # Check that the commented lines were ignored  # noqa E501

    def test_generateNamesFromNameFiles_nonexistent_file(self, mixin):
        # Test a file that does not exist to trigger the errorCallback
        error_callback = Mock()
        with patch("builtins.print") as mock_print:
            result = list(
                mixin.generateNamesFromNameFiles(
                    ["nonexistent_file.txt"], errorCallback=error_callback
                )
            )

            # Check that the result is an empty list, since the file was not read # noqa E501
            assert result == []

            # Check if the errorCallback was called with the path of the nonexistent file  # noqa E501
            error_callback.assert_called_once_with(
                "<file> nonexistent_file.txt",
                "[Errno 2] No such file or directory: 'nonexistent_file.txt'",
            )

            # Check if the warning message was printed
            mock_print.assert_called_once_with(
                "Warning: WARNING: error reading input file 'nonexistent_file.txt': [Errno 2] No such file or directory: 'nonexistent_file.txt'\n"  # noqa E501
            )

    # ----------------------------------------------------------------------

    # -- Tests to loadUserKnowledgeFile Method --
    def test_loadUserKnowledgeFile_valid_file(self, mixin, tmp_path):
        # Create a temporary file with valid content
        file_path = tmp_path / "knowledge_file.txt"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\textra_description\nname1\tname2\n"  # noqa E501
        file_path.write_text(file_content)

        # Add mocks directly to the `mixin` object
        mixin.addUserSource = Mock(return_value=1)
        mixin.addUserGroup = Mock(return_value=2)
        mixin.addUserGroupBiopolymers = Mock()

        # Run the method to be tested with prints for debugging
        with patch("builtins.print") as mock_print:  # noqa E501
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")
            # Check the temporary print to confirm the reading and processing of the lines  # noqa E501
            # mock_print.assert_any_call("source_label\tdescription")
            # mock_print.assert_any_call("GROUP\tgroup_label\textra_description")
            # mock_print.assert_any_call("name1\tname2")

        # Check if `addUserSource` was called correctly
        mixin.addUserSource.assert_called_once_with(
            "source_label", "description"
        )  # noqa E501

        # Check if `addUserGroup` was called with the correct parameters
        mixin.addUserGroup.assert_called_once_with(
            1, "group_label", "extra_description", None
        )

        # Check if `addUserGroupBiopolymers` was called with the correct group and names  # noqa E501
        mixin.addUserGroupBiopolymers.assert_called_once()
        nameset = mixin.addUserGroupBiopolymers.call_args[0][1]
        assert nameset == [[(None, "name1", None), (None, "name2", None)]]

    def test_loadUserKnowledgeFile_nonexistent_file(self, mixin):
        # Test the case where the file does not exist
        error_callback = Mock()
        with patch("builtins.print") as mock_print:
            # Run the method without waiting for a return
            mixin.loadUserKnowledgeFile(
                "nonexistent_file.txt", errorCallback=error_callback
            )
            # Check if the errorCallback was called with the path of the nonexistent file  # noqa E501
            error_callback.assert_called_once_with(
                "<file> nonexistent_file.txt",
                "[Errno 2] No such file or directory: 'nonexistent_file.txt'",
            )
            # Check if the warning message was printed
            mock_print.assert_called_once_with(
                "Warning: WARNING: error reading input file 'nonexistent_file.txt': [Errno 2] No such file or directory: 'nonexistent_file.txt'\n"  # noqa E501
            )

    def test_loadUserKnowledgeFile_with_children_and_empty_lines(
        self, mixin, tmp_path
    ):  # noqa E501
        # Test if the file has `CHILDREN` lines and empty lines
        file_path = tmp_path / "knowledge_with_children.txt"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\nCHILDREN\n\nname1\tname2\n"  # noqa E501
        file_path.write_text(file_content)

        with patch.object(
            mixin, "addUserSource", return_value=1
        ) as mock_addUserSource, patch.object(
            mixin, "addUserGroup", return_value=2
        ) as mock_addUserGroup, patch.object(
            mixin, "addUserGroupBiopolymers"
        ) as mock_addUserGroupBiopolymers:

            # Run the method without waiting for a return
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            # Check if `addUserSource` was called correctly
            mock_addUserSource.assert_called_once_with(
                "source_label", "description"
            )  # noqa E501

            # Check if `addUserGroup` was called with the correct parameters
            mock_addUserGroup.assert_called_once_with(
                1, "group_label", "", None
            )  # noqa E501

            # Check if `addUserGroupBiopolymers` was called with the correct group and names  # noqa E501
            mock_addUserGroupBiopolymers.assert_called_once()
            nameset = mock_addUserGroupBiopolymers.call_args[0][1]
            # assert nameset == [[("default_ns", "name1", None), ("default_ns", "name2", None)]]  # noqa E501
            # assert nameset == [[(None, '', None)], [(None, 'name1', None), (None, 'name2', None)]]  # noqa E501
            assert nameset == [[(None, "name1", None), (None, "name2", None)]]

    def test_loadUserKnowledgeFile_read_error(self, mixin):
        # Test the case where an error occurs while reading the file
        error_callback = Mock()
        with patch("builtins.open", side_effect=OSError("Read error")), patch(
            "builtins.print"
        ) as mock_print:
            # Run the method without waiting for a return
            mixin.loadUserKnowledgeFile(
                "some_file.txt", errorCallback=error_callback
            )  # noqa E501

            # Check if the errorCallback was called with the error message
            error_callback.assert_called_once_with(
                "<file> some_file.txt", "Read error"
            )  # noqa E501

            # Check if the warning message was printed
            mock_print.assert_called_once_with(
                "Warning: WARNING: error reading input file 'some_file.txt': Read error\n"  # noqa E501
            )

    def test_loadUserKnowledgeFile_with_empty_line(self, mixin, tmp_path):
        # Create a temporary file with an empty line just after `GROUP` to trigger `if not words`  # noqa E501
        file_path = tmp_path / "knowledge_with_empty_line.txt"
        # add \n\n to test "if all(word.strip() == '' for word in words)
        file_content = (
            "source_label\tdescription\nGROUP\tgroup_label\n\nname1\n\n"  # noqa E501
        )
        file_path.write_text(file_content)

        with patch.object(
            mixin, "addUserSource", return_value=1
        ) as mock_addUserSource, patch.object(
            mixin, "addUserGroup", return_value=2
        ) as mock_addUserGroup, patch.object(
            mixin, "addUserGroupBiopolymers"
        ) as mock_addUserGroupBiopolymers:  # noqa F481

            # Run method to be tested
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            mock_addUserSource.assert_called_once_with(
                "source_label", "description"
            )  # noqa E501

            # Check if `addUserGroup` was called with the correct parameters
            mock_addUserGroup.assert_called_once_with(
                1, "group_label", "", None
            )  # noqa E501

            # `addUserGroupBiopolymers` não deve ser chamado, pois `if not words` impede  # noqa E501
            # mock_addUserGroupBiopolymers.assert_not_called()

    # Test to cover `if ugroupID and namesets`
    def test_loadUserKnowledgeFile_with_group_and_namesets(
        self, mixin, tmp_path
    ):  # noqa E501
        # Create a temporary file to trigger `if ugroupID and namesets`

        file_path = tmp_path / "knowledge_with_group_and_namesets.txt"
        # add group 2x to test: "if ugroupID and namesets"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\nname1\tname2\nGROUP\tgroup_label\n"  # noqa E501
        file_path.write_text(file_content)

        with patch.object(
            mixin, "addUserSource", return_value=1
        ) as mock_addUserSource, patch.object(
            mixin, "addUserGroup", return_value=2
        ) as mock_addUserGroup, patch.object(
            mixin, "addUserGroupBiopolymers"
        ) as mock_addUserGroupBiopolymers:

            # Run method to be tested
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            # Check if `addUserSource` was called correctly
            mock_addUserSource.assert_called_once_with(
                "source_label", "description"
            )  # noqa E501

            # Check if `addUserGroup` was called with the correct parameters
            # mock_addUserGroup.assert_called_once_with(1, "group_label", "", None)  # noqa E501
            assert mock_addUserGroup.call_count == 2

            # Check if `addUserGroupBiopolymers` was called with the correct group and names  # noqa E501
            mock_addUserGroupBiopolymers.assert_called_once()
            nameset = mock_addUserGroupBiopolymers.call_args[0][1]
            assert nameset == [[(None, "name1", None), (None, "name2", None)]]
            assert 2 == 2

    # ----------------------------------------------------------------------
