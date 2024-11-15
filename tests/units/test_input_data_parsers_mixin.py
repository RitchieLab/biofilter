import pytest
from biofilter_modules.mixins.input_data_parsers_mixin import InputDataParsersMixin


class TestInputDataParsersMixin:
    @pytest.fixture
    def mixin(self):
        class MockLoki:
            def getUCSChgByGRCh(self, grchBuild):
                return 38 if grchBuild == 37 else None

            def generateGRChByUCSChg(self, ucscBuild):
                return [37] if ucscBuild == 38 else []

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
                return [(rs, posextra, "chr1", 1000) for rs, posextra in genMergeFormat]

            def hasLiftOverChains(self, ucscBuildOld, ucscBuildNew):
                # Simulando a existência de cadeias de liftOver entre versões.
                return True

            def generateLiftOverLoci(
                self, ucscBuildOld, ucscBuildNew, loci, tally=None, errorCallback=None
            ):
                # Retornando loci simulados após o liftOver
                return [
                    (label, chm, pos + 100, extra) for label, chm, pos, extra in loci
                ]

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

        return TestClass()

    # -- Tests to getInputGenomeBuilds Method --
    def test_getInputGenomeBuilds(self, mixin):
        assert mixin.getInputGenomeBuilds(37, 38) == (37, 38)
        assert mixin.getInputGenomeBuilds(None, 38) == (37, 38)
        assert mixin.getInputGenomeBuilds(37, None) == (37, 38)

    def test_getInputGenomeBuilds_invalid_grch_and_ucsc(self, mixin):
        with pytest.raises(SystemExit):
            mixin.getInputGenomeBuilds(37, 39)

    def test_getInputGenomeBuilds_no_grch_no_ucsc(self, mixin):
        assert mixin.getInputGenomeBuilds(None, None) == (None, None)

    # -- Tests to generateCurrentRSesByRSes Method --
    def test_generateMergedFilteredSNPs(self, mixin):
        snps = [(1, "extra1"), (2, "extra2")]
        result = list(mixin.generateMergedFilteredSNPs(snps))
        assert result == [("1", "extra1", "2"), ("2", "extra2", "3")]

    def test_generateMergedFilteredSNPs_ambiguous_snps_allowed(self, mixin):
        mixin._options.allow_ambiguous_snps = "yes"
        snps = [(1, "extra1"), (2, "extra2")]
        result = list(mixin.generateMergedFilteredSNPs(snps))
        assert result == [(1, "extra1", 2), (2, "extra2", 3)]

    def test_generateMergedFilteredSNPs_ambiguous_snps_not_allowed(self, mixin):
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
            mixin.generateMergedFilteredSNPs(snps, errorCallback=error_callback)
        )
        assert result == [("1", "extra1", "2"), ("2", "extra2", "3")]

    # -- Tests to generateSNPLociByRSes Method --
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

    def test_generateRSesFromText_with_error_callback(self, mixin):
        def error_callback(line, message):
            print(f"Error: {message} in line {line}")

        lines = ["invalid_line", "rs2 extra2"]
        result = list(mixin.generateRSesFromText(lines, errorCallback=error_callback))
        assert result == [(2, "extra2")]

    def test_generateRSesFromText_empty_lines(self, mixin):
        lines = ["", "rs2 extra2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(2, "extra2")]

    def test_generateRSesFromText_no_extra_info(self, mixin):
        lines = ["rs1", "rs2"]
        result = list(mixin.generateRSesFromText(lines))
        assert result == [(1, None), (2, None)]

    # -- Tests to generateRSesFromRSFiles Method --
    def test_generateRSesFromRSFiles(self, mixin, tmp_path):
        file = tmp_path / "rs_file.txt"
        file.write_text("rs1 extra1\nrs2 extra2\n")
        result = list(mixin.generateRSesFromRSFiles([str(file)]))
        assert result == [(1, "extra1"), (2, "extra2")]

    # -- Tests to generateLociFromText Method --
    def test_generateLociFromText(self, mixin):
        lines = ["chr1 1000", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromMapFiles(self, mixin, tmp_path):
        file = tmp_path / "map_file.txt"
        file.write_text("chr1 1000\nchr1 2000\n")
        result = list(mixin.generateLociFromMapFiles([str(file)]))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    # -- Tests to generateLiftOverLoci Method --
    def test_generateLiftOverLoci(self, mixin):
        loci = [("label1", 1, 1000, "extra1"), ("label2", 1, 2000, "extra2")]
        result = mixin.generateLiftOverLoci("hg18", "hg19", loci)
        assert result == [("label1", 1, 1100, "extra1"), ("label2", 1, 2100, "extra2")]

    # -- Tests to generateRegionsFromText Method --
    def test_generateRegionsFromText(self, mixin):
        lines = ["chr1 1000 2000", "chr1 3000 4000"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [
            ("chr1:1000-2000", 1, 1000, 2000, None),
            ("chr1:3000-4000", 1, 3000, 4000, None),
        ]

    # -- Tests to generateRegionsFromFiles Method --
    def test_generateRegionsFromFiles(self, mixin, tmp_path):
        file = tmp_path / "regions_file.txt"
        file.write_text("chr1 1000 2000\nchr1 3000 4000\n")
        result = list(mixin.generateRegionsFromFiles([str(file)]))
        assert result == [
            ("chr1:1000-2000", 1, 1000, 2000, None),
            ("chr1:3000-4000", 1, 3000, 4000, None),
        ]
        # -------- ultima

    # -- Tests to generateLociFromText Method --
    def test_generateLociFromText(self, mixin):
        lines = ["chr1 1000", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_with_separator(self, mixin):
        lines = ["chr1,1000", "chr1,2000"]
        result = list(mixin.generateLociFromText(lines, separator=","))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_with_offset(self, mixin):
        lines = ["chr1 1000", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines, applyOffset=True))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_with_error_callback(self, mixin):
        def error_callback(line, message):
            print(f"Error: {message} in line {line}")

        lines = ["invalid_line", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines, errorCallback=error_callback))
        assert result == [("chr1:2000", 1, 2000, None)]

    # def test_generateLociFromText_invalid_chromosome(self, mixin):
    #     # lines = ["chrX 1000", "chr1 2000"]
    #     lines = ["chrX 1000", "chrX 2000"]
    #     with pytest.raises(Exception, match="invalid chromosome 'X'"):
    #         list(mixin.generateLociFromText(lines))

    # def test_generateLociFromText_incomplete_lines(self, mixin):
    #     lines = ["chr1", "chr1 2000"]
    #     with pytest.raises(Exception, match="not enough columns"):
    #         list(mixin.generateLociFromText(lines))

    # def test_generateLociFromText_with_extra_data(self, mixin):
    #     lines = ["chr1 1000 label extra", "chr1 2000 label extra"]
    #     result = list(mixin.generateLociFromText(lines))
    #     assert result == [("label", 1, 1000, "extra"), ("label", 1, 2000, "extra")]

    def test_generateLociFromText_with_na_position(self, mixin):
        lines = ["chr1 NA", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:NA", 1, None, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_with_dash_position(self, mixin):
        lines = ["chr1 -", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:-", 1, None, None), ("chr1:2000", 1, 2000, None)]

    # -- FIM ----------------------------------------------------------------
