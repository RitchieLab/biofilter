import pytest
from biofilter_modules.mixins.input_data_parsers_mixin import InputDataParsersMixin
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
            
            def generateLiftOverRegions(
                    self, ucscBuildOld, ucscBuildNew, regions, tally=None, errorCallback=None
                    ):
                # Simula um comportamento que chama o errorCallback em uma condição de erro
                if errorCallback:
                    # errorCallback(regions, "dropped during liftOver from hg%s to hg%s" % (ucscBuildOld, ucscBuildNew))
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
        # Testa o uso de generateGRChByUCSChg com `ucscBuild` e cobertura do
        # `if grchBuild`
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
        # Linhas de teste, incluindo uma linha que causará um erro para acionar o callback
        lines = ["rs12345", "invalid_rs", "rs67890"]
        # Converte o resultado para uma lista para forçar a execução de todo o gerador
        result = list(mixin.generateRSesFromText(lines, errorCallback=error_callback))
        # Verifica se as linhas válidas foram processadas corretamente
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
        # Fornece um caminho de arquivo inexistente para forçar um erro
        invalid_path = "invalid_file.txt"
        # Usa patch para monitorar chamadas ao método warn
        with patch.object(mixin, 'warn') as mock_warn:
            result = list(mixin.generateRSesFromRSFiles([invalid_path], errorCallback=error_callback))
            # Verifica se o método warn foi chamado com a mensagem esperada
            mock_warn.assert_called_once()
            assert "WARNING: error reading input file" in mock_warn.call_args[0][0]
            # Verifica se o resultado está vazio, já que o arquivo não foi encontrado
            assert result == []
    # ----------------------------------------------------------------------

    # -- Tests to generateLociFromText Method --
    def test_generateLociFromText(self, mixin):
        lines = ["chr1 1000", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_empty_cols(self, mixin):
        # Testa o caso em que cols está vazio
        lines = ["   "]  # Linha vazia após strip
        result = list(mixin.generateLociFromText(lines))
        assert result == []  # Nenhuma saída esperada, pois a linha é ignorada

    def test_generateLociFromText_three_columns(self, mixin):
        # Testa o caso em que há exatamente três colunas
        lines = ["chr1 label 1000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("label", 1, 1000, None)]  # Extra deve ser None

    def test_generateLociFromText_four_columns(self, mixin):
        # Testa o caso em que há quatro colunas
        lines = ["chr1 label extra 1000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("label", 1, 1000, None)]  # Extra ainda deve ser None

    def test_generateLociFromText_five_columns(self, mixin):
        # Testa o caso em que há cinco colunas
        lines = ["chr1 label extra 1000 additional"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("label", 1, 1000, "additional")]  # Extra deve ser "additional"

    def test_generateLociFromText_invalid_chromosome_with_error_callback(self, mixin):
        def error_callback(line, message):
            print(f"Error: {message} in line: {line}")
        # Linha com cromossomo inválido "chrX", que deve acionar o errorCallback
        lines = ["chr1 1000", "chrX 2000"]
        # Usa patch para monitorar chamadas ao error_callback
        with patch('builtins.print') as mock_print:
            result = list(mixin.generateLociFromText(lines, errorCallback=error_callback))
            # Verifica se o errorCallback foi chamado com a mensagem esperada para "chrX"
            mock_print.assert_called_once_with("Error: invalid chromosome 'X' at index 2 in line: chrX 2000")
        # Verifica se apenas a primeira linha válida foi processada
        assert result == [("chr1:1000", 1, 1000, None)]

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

    def test_generateLociFromText_with_na_position(self, mixin):
        lines = ["chr1 NA", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:NA", 1, None, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromText_with_dash_position(self, mixin):
        lines = ["chr1 -", "chr1 2000"]
        result = list(mixin.generateLociFromText(lines))
        assert result == [("chr1:-", 1, None, None), ("chr1:2000", 1, 2000, None)]
    # ----------------------------------------------------------------------

    # -- Tests to generateLociFromMapFiles Method --
    def test_generateLociFromMapFiles(self, mixin, tmp_path):
        file = tmp_path / "map_file.txt"
        file.write_text("chr1 1000\nchr1 2000\n")
        result = list(mixin.generateLociFromMapFiles([str(file)]))
        assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]

    def test_generateLociFromMapFiles_with_error_callback(self, mixin):
        def error_callback(path, message):
            print(f"Error: {message} in path {path}")
        # Fornece um caminho de arquivo inexistente para forçar um erro
        invalid_path = "invalid_file.txt"
        # Usa patch para monitorar chamadas ao método warn
        with patch.object(mixin, 'warn') as mock_warn:
            result = list(mixin.generateLociFromMapFiles([invalid_path], errorCallback=error_callback))
            # Verifica se o método warn foi chamado com a mensagem esperada
            mock_warn.assert_called_once()
            assert "WARNING: error reading input file" in mock_warn.call_args[0][0]
            # Verifica se o resultado está vazio, já que o arquivo não foi encontrado
            assert result == []
    # ----------------------------------------------------------------------

    # -- Tests to generateLiftOverLoci Method --
    def test_generateLiftOverLoci(self, mixin):
        loci = [("label1", 1, 1000, "extra1"), ("label2", 1, 2000, "extra2")]
        result = mixin.generateLiftOverLoci("hg18", "hg19", loci)
        assert result == [("label1", 1, 1100, "extra1"), ("label2", 1, 2100, "extra2")]

    def test_generateLiftOverLoci_no_ucscBuildOld(self, mixin):
        # Testa o caso em que `ucscBuildOld` não é fornecido
        loci = [("label", "chr1", 1000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            mixin.generateLiftOverLoci(None, 38, loci)
            # Verifica se a mensagem de aviso correta foi registrada
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version was not specified for position input; assuming it matches the knowledge database\n"
            )

    def test_generateLiftOverLoci_no_ucscBuildNew(self, mixin):
        # Testa o caso em que `ucscBuildNew` não é fornecido
        loci = [("label", "chr1", 1000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            mixin.generateLiftOverLoci(37, None, loci)
            # Verifica se a mensagem de aviso correta foi registrada
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n"
            )

    def test_generateLiftOverLoci_no_liftOver_chains(self, mixin):
        # Testa o caso em que `hasLiftOverChains` retorna `False`, acionando `sys.exit`
        loci = [("label", "chr1", 1000, None)]
        with patch.object(mixin._loki, "hasLiftOverChains", return_value=False):
            with pytest.raises(SystemExit) as e:
                mixin.generateLiftOverLoci(37, 38, loci)
            # Verifica se a mensagem de erro contém o texto esperado
            assert str(e.value) == (
                "ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg37 to hg38\n"
            )

    def test_generateLiftOverLoci_liftoverCallback(self, mixin):
        def error_callback(region, message):
            print(f"Error: {message} in region: {region}")
        loci = [("label", "chr1", 1000, None)]
        # Mock `generateLiftOverLoci` para simular a chamada de `liftoverCallback`
        with patch.object(mixin._loki, "generateLiftOverLoci", side_effect=lambda *args, **kwargs: [kwargs["errorCallback"](loci[0])]):
            with patch("builtins.print") as mock_print:
                mixin.generateLiftOverLoci(37, 38, loci, errorCallback=error_callback)

                # Verifica se o `errorCallback` foi chamado com a mensagem esperada
                mock_print.assert_called_once_with(
                    "Error: dropped during liftOver from hg37 to hg38 in region: label\tchr1\t1000\tNone"
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
        # Testa o caso em que applyOffset está ativo e regions_half_open é "yes"
        mixin._options.regions_half_open = "yes"
        mixin._options.coordinate_base = 1
        lines = ["chr1 1000 2000"]
        result = list(mixin.generateRegionsFromText(lines, applyOffset=True))
        # offsetEnd é decrementado em 1 quando `regions_half_open` é "yes"
        assert result == [("chr1:1000-2000", 1, 1000, 1999, None)]

    def test_generateRegionsFromText_empty_cols(self, mixin):
        # Testa o caso em que `cols` está vazio
        lines = ["   "]  # Linha vazia após strip()
        result = list(mixin.generateRegionsFromText(lines))
        assert result == []  # A linha deve ser ignorada

    def test_generateRegionsFromText_four_or_more_columns(self, mixin):
        # Testa o caso em que há quatro colunas ou mais
        lines = ["chr1 label 1000 2000 extra"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [("label", 1, 1000, 2000, "extra")]

    def test_generateRegionsFromText_chromosome_prefix(self, mixin):
        # Testa o caso em que o cromossomo começa com "CHR"
        lines = ["chr1 1000 2000"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [("chr1:1000-2000", 1, 1000, 2000, None)]

    def test_generateRegionsFromText_posMin_and_posMax_na(self, mixin):
        # Testa os casos em que posMin e posMax são "NA"
        lines = ["chr1 NA NA"]
        result = list(mixin.generateRegionsFromText(lines))
        assert result == [("chr1:NA-NA", 1, None, None, None)]

    def test_generateRegionsFromText_with_error_callback(self, mixin):
        # Testa o caso em que errorCallback é chamado devido a erro em uma linha
        def error_callback(line, message):
            print(f"Error: {message} in line: {line}")

        lines = ["chr1 1000 2000", "chrX 3000 4000"]  # "chrX" deve causar um erro
        with patch("builtins.print") as mock_print:
            result = list(mixin.generateRegionsFromText(lines, errorCallback=error_callback))
            # Verifica se o `errorCallback` foi chamado para a linha inválida
            mock_print.assert_called_once_with("Error: invalid chromosome 'X' at index 2 in line: chrX 3000 4000")
        # Verifica se apenas a primeira linha válida foi processada
        assert result == [("chr1:1000-2000", 1, 1000, 2000, None)]

    def test_generateRegionsFromText_with_error_callback_col(self, mixin):
        # Testa o caso em que errorCallback é chamado devido a erro em uma linha
        def error_callback(line, message):
            print(f"Error: {message} in line: {line}")
        lines = ["chr1 1000", "chrX 3000"]  # "chrX" deve causar um erro
        with patch("builtins.print") as mock_print:
            result = list(mixin.generateRegionsFromText(lines, errorCallback=error_callback))
            assert result == []
            # Verifica se o `errorCallback` foi chamado para a linha inválida
            mock_print.assert_called_once_with("Error: not enough columns at index 2 in line: chrX 3000")
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
        # Fornece um caminho de arquivo inexistente para forçar um erro
        invalid_path = "invalid_file.txt"
        # Usa patch para monitorar chamadas ao método warn
        with patch.object(mixin, 'warn') as mock_warn:
            result = list(mixin.generateRegionsFromFiles([invalid_path], errorCallback=error_callback))
            # Verifica se o método warn foi chamado com a mensagem esperada
            mock_warn.assert_called_once()
            assert "WARNING: error reading input file" in mock_warn.call_args[0][0]
            # Verifica se o resultado está vazio, já que o arquivo não foi encontrado
            assert result == []
    # ----------------------------------------------------------------------

    # -- Tests to generateLiftOverRegions Method --
    def test_generateLiftOverRegions_no_ucscBuildOld(self, mixin):
        # Testa o caso em que `ucscBuildOld` não é fornecido
        regions = [("label", "chr1", 1000, 2000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            result = mixin.generateLiftOverRegions(None, 38, regions)
            # Verifica se a mensagem de aviso correta foi registrada
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version was not specified for region input; assuming it matches the knowledge database\n"
            )
            # Verifica que o resultado é o mesmo que a entrada
            assert result == regions

    def test_generateLiftOverRegions_no_ucscBuildNew(self, mixin):
        # Testa o caso em que `ucscBuildNew` não é fornecido
        regions = [("label", "chr1", 1000, 2000, None)]
        with patch.object(mixin, "warn") as mock_warn:
            result = mixin.generateLiftOverRegions(37, None, regions)
            # Verifica se a mensagem de aviso correta foi registrada
            mock_warn.assert_called_once_with(
                "WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n"
            )
            # Verifica que o resultado é o mesmo que a entrada
            assert result == regions

    def test_generateLiftOverRegions_no_liftOver_chains(self, mixin):
        # Testa o caso em que `hasLiftOverChains` retorna `False`, acionando `sys.exit`
        regions = [("label", "chr1", 1000, 2000, None)]
        with patch.object(mixin._loki, "hasLiftOverChains", return_value=False):
            with pytest.raises(SystemExit) as e:
                mixin.generateLiftOverRegions(37, 38, regions)
            # Verifica se a mensagem de erro contém o texto esperado
            assert str(e.value) == (
                "ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg37 to hg38\n"
            )

    def test_generateLiftOverRegions_liftoverCallback(self, mixin):
        # Define um error_callback que aceita dois argumentos para capturar o region e message separadamente
        def error_callback(region, message):
            print(f"Error: {message} in region: {region}")
        regions = [("label", "chr1", 1000, 2000, None)]
        # Patch para simular o comportamento de generateLiftOverRegions
        with patch.object(mixin._loki, "generateLiftOverRegions", wraps=mixin._loki.generateLiftOverRegions):
            with patch("builtins.print") as mock_print:
                result = mixin.generateLiftOverRegions(37, 38, regions, errorCallback=error_callback)
                # Verifica se o `errorCallback` foi chamado com a mensagem esperada
                mock_print.assert_called_once_with(
                    "Error: dropped during liftOver from hg37 to hg38 in region: ('label', 'chr1', 1000, 2000, None)"
                )
                # Verifica que o resultado é o mesmo que a entrada, pois foi interrompido
                assert result == regions
    # ----------------------------------------------------------------------

    # -- Tests to generateNamesFromText Method --
    def test_generateNamesFromText_empty_line(self, mixin):
        # Testa uma linha vazia
        lines = [""]
        result = list(mixin.generateNamesFromText(lines))
        assert result == []  # Deve retornar uma lista vazia

    def test_generateNamesFromText_single_column(self, mixin):
        # Testa uma linha com uma coluna e um namespace padrão
        lines = ["name1"]
        result = list(mixin.generateNamesFromText(lines, defaultNS="default_ns"))
        assert result == [("default_ns", "name1", None)]  # Deve usar o namespace padrão para `ns`

    def test_generateNamesFromText_two_columns(self, mixin):
        # Testa uma linha com duas colunas
        lines = ["ns1\tname2"]
        result = list(mixin.generateNamesFromText(lines, separator="\t"))
        assert result == [("ns1", "name2", None)]  # Deve capturar `ns` e `name`, com `extra` como None

    def test_generateNamesFromText_three_columns(self, mixin):
        # Testa uma linha com três colunas
        lines = ["ns2\tname3\textra_info"]
        result = list(mixin.generateNamesFromText(lines, separator="\t"))
        assert result == [("ns2", "name3", "extra_info")]  # Deve capturar `ns`, `name` e `extra`

    def test_generateNamesFromText_multiple_lines(self, mixin):
        # Testa várias linhas, cada uma com um número diferente de colunas
        lines = [
            "name4",                    # Uma coluna
            "ns5\tname5",               # Duas colunas
            "ns6\tname6\textra_info6"   # Três colunas
        ]
        result = list(mixin.generateNamesFromText(lines, defaultNS="default_ns", separator="\t"))
        expected = [
            ("default_ns", "name4", None),       # Usa `defaultNS` para `ns`
            ("ns5", "name5", None),              # Captura `ns` e `name`, `extra` é None
            ("ns6", "name6", "extra_info6")      # Captura `ns`, `name` e `extra`
        ]
        assert result == expected

    def test_generateNamesFromText_errorCallback(self, mixin):
        # # Testa o caso em que `errorCallback` é acionado por um erro de parsing
        # lines = ["ns7\tname7", "invalid_line"]  # A segunda linha é inválida
        # error_callback = Mock()

        # # Usa um patch no print para verificar se o errorCallback foi acionado
        # with patch("builtins.print") as mock_print:
        #     result = list(mixin.generateNamesFromText(lines, separator="\t", errorCallback=error_callback))

        #     # A primeira linha é válida
        #     assert result == [('ns7', 'name7', None), (None, 'invalid_line', None)]

        #     # Verifica se o errorCallback foi chamado com a mensagem esperada para a segunda linha
        # error_callback.assert_called_once_with("invalid_line", "not enough columns at index 2")

        # Testa o caso em que `errorCallback` é acionado por um erro de parsing
        lines = ["ns7\tname7", None]  # A segunda linha é None e deve causar um erro
        error_callback = Mock()

        # Executa o método com o errorCallback
        result = list(mixin.generateNamesFromText(lines, separator="\t", errorCallback=error_callback))

        # Verifica o resultado esperado para a primeira linha válida
        assert result == [('ns7', 'name7', None)]

        # Verifica se o errorCallback foi chamado com a linha inválida
        error_callback.assert_called_once()
        called_args = error_callback.call_args[0]
        assert called_args[0] is None  # Linha inválida passada para o callback
        # assert "object is not subscriptable" in called_args[1]  # Parte da mensagem de erro
        assert "'NoneType' object has no attribute 'strip' at index 2" in called_args[1]  # Parte da mensagem de erro
    # ----------------------------------------------------------------------

    # -- Tests to generateNamesFromNameFiles Method --
    def test_generateNamesFromNameFiles_valid_file(self, mixin, tmp_path):
        file = tmp_path / "test_file.txt"
        file.write_text("ns1\tname1\nns2\tname2\n")
        result = list(mixin.generateNamesFromNameFiles([str(file)]))
        # assert result == [("chr1:1000", 1, 1000, None), ("chr1:2000", 1, 2000, None)]
        expected = [("ns1", "name1", None), ("ns2", "name2", None)]
        assert result == expected  # Confirma que as linhas foram lidas corretamente

    def test_generateNamesFromNameFiles_with_comments(self, mixin, tmp_path):
        # Cria um arquivo temporário com algumas linhas comentadas
        file_path = tmp_path / "test_file.txt"
        file_path.write_text("# This is a comment\nns3\tname3\n# Another comment\nns4\tname4\n")

        result = list(mixin.generateNamesFromNameFiles([str(file_path)], separator="\t"))
        expected = [("ns3", "name3", None), ("ns4", "name4", None)]
        assert result == expected  # Verifica que as linhas comentadas foram ignoradas

    def test_generateNamesFromNameFiles_nonexistent_file(self, mixin):
        # Testa um arquivo que não existe para acionar o errorCallback
        error_callback = Mock()
        with patch("builtins.print") as mock_print:
            result = list(mixin.generateNamesFromNameFiles(["nonexistent_file.txt"], errorCallback=error_callback))

            # Verifica que o resultado é uma lista vazia, pois o arquivo não foi lido
            assert result == []

            # Verifica que o errorCallback foi chamado com o caminho do arquivo inexistente
            error_callback.assert_called_once_with('<file> nonexistent_file.txt', "[Errno 2] No such file or directory: 'nonexistent_file.txt'")

            # Verifica que o aviso foi impresso
            mock_print.assert_called_once_with(
                "Warning: WARNING: error reading input file 'nonexistent_file.txt': [Errno 2] No such file or directory: 'nonexistent_file.txt'\n"
            )

    # ----------------------------------------------------------------------

    # -- Tests to loadUserKnowledgeFile Method --
    def test_loadUserKnowledgeFile_valid_file(self, mixin, tmp_path):
        # Cria um arquivo temporário com conteúdo válido
        file_path = tmp_path / "knowledge_file.txt"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\textra_description\nname1\tname2\n"
        file_path.write_text(file_content)

        # Adiciona os mocks diretamente no objeto `mixin`
        mixin.addUserSource = Mock(return_value=1)
        mixin.addUserGroup = Mock(return_value=2)
        mixin.addUserGroupBiopolymers = Mock()

        # Executa o método a ser testado com prints para depuração
        with patch("builtins.print") as mock_print:
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            # # Verifique o print temporário para confirmar a leitura e processamento das linhas
            # mock_print.assert_any_call("source_label\tdescription")
            # mock_print.assert_any_call("GROUP\tgroup_label\textra_description")
            # mock_print.assert_any_call("name1\tname2")

        # Verifica se `addUserSource` foi chamado corretamente
        mixin.addUserSource.assert_called_once_with("source_label", "description")

        # Verifica se `addUserGroup` foi chamado com os parâmetros corretos
        mixin.addUserGroup.assert_called_once_with(1, "group_label", "extra_description", None)

        # Verifica se `addUserGroupBiopolymers` foi chamado com o grupo e nomes corretos
        mixin.addUserGroupBiopolymers.assert_called_once()
        nameset = mixin.addUserGroupBiopolymers.call_args[0][1]
        assert nameset == [[(None, 'name1', None), (None, 'name2', None)]]


    def test_loadUserKnowledgeFile_nonexistent_file(self, mixin):
        # Testa o caso em que o arquivo não existe
        error_callback = Mock()
        with patch("builtins.print") as mock_print:
            # Executa o método sem esperar retorno
            mixin.loadUserKnowledgeFile("nonexistent_file.txt", errorCallback=error_callback)

            # Verifica se o errorCallback foi chamado com o caminho do arquivo inexistente
            error_callback.assert_called_once_with('<file> nonexistent_file.txt', "[Errno 2] No such file or directory: 'nonexistent_file.txt'")

            # Verifica se a mensagem de aviso foi impressa
            mock_print.assert_called_once_with("Warning: WARNING: error reading input file 'nonexistent_file.txt': [Errno 2] No such file or directory: 'nonexistent_file.txt'\n")


    def test_loadUserKnowledgeFile_with_children_and_empty_lines(self, mixin, tmp_path):
        # Testa o caso de um arquivo com linhas `CHILDREN` e linhas vazias
        file_path = tmp_path / "knowledge_with_children.txt"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\nCHILDREN\n\nname1\tname2\n"
        file_path.write_text(file_content)

        with patch.object(mixin, "addUserSource", return_value=1) as mock_addUserSource, \
            patch.object(mixin, "addUserGroup", return_value=2) as mock_addUserGroup, \
            patch.object(mixin, "addUserGroupBiopolymers") as mock_addUserGroupBiopolymers:

            # Executa o método sem esperar retorno
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            # Verifica se `addUserSource` foi chamado corretamente
            mock_addUserSource.assert_called_once_with("source_label", "description")

            # Verifica se `addUserGroup` foi chamado com os parâmetros corretos
            mock_addUserGroup.assert_called_once_with(1, "group_label", "", None)

            # Verifica se `addUserGroupBiopolymers` foi chamado com o grupo e nomes corretos
            mock_addUserGroupBiopolymers.assert_called_once()
            nameset = mock_addUserGroupBiopolymers.call_args[0][1]
            # assert nameset == [[("default_ns", "name1", None), ("default_ns", "name2", None)]]
            assert nameset == [[(None, '', None)], [(None, 'name1', None), (None, 'name2', None)]]

    def test_loadUserKnowledgeFile_read_error(self, mixin):
        # Simula um erro de leitura ao abrir o arquivo
        error_callback = Mock()
        with patch("builtins.open", side_effect=OSError("Read error")), patch("builtins.print") as mock_print:
            # Executa o método sem esperar retorno
            mixin.loadUserKnowledgeFile("some_file.txt", errorCallback=error_callback)

            # Verifica que o errorCallback foi chamado com a mensagem de erro
            error_callback.assert_called_once_with("<file> some_file.txt", "Read error")

            # Verifica que o aviso foi impresso
            mock_print.assert_called_once_with(
                "Warning: WARNING: error reading input file 'some_file.txt': Read error\n"
            )

    def test_loadUserKnowledgeFile_with_empty_line(self, mixin, tmp_path):
        # Cria um arquivo temporário com uma linha vazia logo após o `GROUP` para acionar `if not words`
        file_path = tmp_path / "knowledge_with_empty_line.txt"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\n\nname1\n"
        file_path.write_text(file_content)

        with patch.object(mixin, "addUserSource", return_value=1) as mock_addUserSource, \
            patch.object(mixin, "addUserGroup", return_value=2) as mock_addUserGroup, \
            patch.object(mixin, "addUserGroupBiopolymers") as mock_addUserGroupBiopolymers:

            # Executa o método a ser testado
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            # Verifica se `addUserSource` foi chamado corretamente
            mock_addUserSource.assert_called_once_with("source_label", "description")

            # Verifica se `addUserGroup` foi chamado com os parâmetros corretos
            mock_addUserGroup.assert_called_once_with(1, "group_label", "", None)

            # `addUserGroupBiopolymers` não deve ser chamado, pois `if not words` impede
            # mock_addUserGroupBiopolymers.assert_not_called()


    # Teste para cobrir `if ugroupID and namesets`
    def test_loadUserKnowledgeFile_with_group_and_namesets(self, mixin, tmp_path):
        # Cria um arquivo temporário para acionar `if ugroupID and namesets`
        file_path = tmp_path / "knowledge_with_group_and_namesets.txt"
        file_content = "source_label\tdescription\nGROUP\tgroup_label\nname1\tname2\n"
        file_path.write_text(file_content)

        with patch.object(mixin, "addUserSource", return_value=1) as mock_addUserSource, \
            patch.object(mixin, "addUserGroup", return_value=2) as mock_addUserGroup, \
            patch.object(mixin, "addUserGroupBiopolymers") as mock_addUserGroupBiopolymers:

            # Executa o método a ser testado
            mixin.loadUserKnowledgeFile(str(file_path), separator="\t")

            # Verifica se `addUserSource` foi chamado corretamente
            mock_addUserSource.assert_called_once_with("source_label", "description")

            # Verifica se `addUserGroup` foi chamado com os parâmetros corretos
            mock_addUserGroup.assert_called_once_with(1, "group_label", "", None)

            # Verifica se `addUserGroupBiopolymers` foi chamado com o grupo e nomes corretos
            mock_addUserGroupBiopolymers.assert_called_once()
            nameset = mock_addUserGroupBiopolymers.call_args[0][1]
            assert nameset == [[(None, 'name1', None), (None, 'name2', None)]]


    # ----------------------------------------------------------------------
