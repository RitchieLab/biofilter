from biofilter_modules import argparse_config
import pytest
import argparse


def test_yesno():
    assert argparse_config.yesno("yes") == "yes"
    assert argparse_config.yesno("1") == "yes"
    assert argparse_config.yesno("t") == "yes"
    assert argparse_config.yesno("true") == "yes"
    assert argparse_config.yesno("y") == "yes"
    assert argparse_config.yesno("on") == "yes"
    assert argparse_config.yesno("no") == "no"
    assert argparse_config.yesno("0") == "no"
    assert argparse_config.yesno("f") == "no"
    assert argparse_config.yesno("false") == "no"
    assert argparse_config.yesno("n") == "no"
    assert argparse_config.yesno("off") == "no"

    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.yesno("maybe")
    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.yesno("2")
    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.yesno("yesno")


def test_percent():
    assert argparse_config.percent("50%") == 50
    assert argparse_config.percent("25") == 25
    assert argparse_config.percent("100%") == 100
    assert argparse_config.percent("0%") == 0
    assert argparse_config.percent("75.5%") == 75.5
    assert argparse_config.percent("99.99") == 99.99

    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.percent("101%")
    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.percent("150")
    with pytest.raises(ValueError):
        argparse_config.percent("abc")


def test_zerotoone():
    assert argparse_config.zerotoone("0.0") == 0.0
    assert argparse_config.zerotoone("1.0") == 1.0
    assert argparse_config.zerotoone("0.5") == 0.5
    assert argparse_config.zerotoone("0.99") == 0.99
    assert argparse_config.zerotoone("0") == 0.0
    assert argparse_config.zerotoone("1") == 1.0

    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.zerotoone("-0.1")
    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.zerotoone("1.1")
    with pytest.raises(ValueError):
        argparse_config.zerotoone("abc")


def test_basepairs():
    assert argparse_config.basepairs("1000") == 1000
    assert argparse_config.basepairs("1k") == 1000
    assert argparse_config.basepairs("1kb") == 1000
    assert argparse_config.basepairs("1m") == 1000000
    assert argparse_config.basepairs("1mb") == 1000000
    assert argparse_config.basepairs("1g") == 1000000000
    assert argparse_config.basepairs("1gb") == 1000000000
    assert argparse_config.basepairs("500") == 500
    assert argparse_config.basepairs("500b") == 500

    with pytest.raises(ValueError):
        argparse_config.basepairs("abc")
    with pytest.raises(ValueError):
        argparse_config.basepairs("1tb")


def test_typePZPV():
    assert argparse_config.typePZPV("significant") == "significant"
    assert argparse_config.typePZPV("sig") == "significant"
    assert argparse_config.typePZPV("s") == "significant"
    assert argparse_config.typePZPV("insignificant") == "insignificant"
    assert argparse_config.typePZPV("insig") == "insignificant"
    assert argparse_config.typePZPV("ignore") == "ignore"
    assert argparse_config.typePZPV("ign") == "ignore"

    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.typePZPV("i")
    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.typePZPV("maybe")
    with pytest.raises(argparse.ArgumentTypeError):
        argparse_config.typePZPV("unknown")

    with pytest.raises(argparse.ArgumentTypeError) as excinfo:
        argparse_config.typePZPV("i")
    assert (
        str(excinfo.value) == "ambiguous value: 'i' could match insignificant, ignore"
    )  # noqa


def test_get_parser_version(capsys):
    parser = argparse_config.get_parser("1.0.0")

    # Catch SystemExit when --version is passed, as it calls sys.exit(0)
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--version"])

    # Check if the exit code is 0, indicating a normal exit
    assert excinfo.value.code == 0

    # Capture the standard output and check if it contains the correct version
    captured = capsys.readouterr()
    assert "LOKI version" in captured.out
    assert "SQLite version" in captured.out
    assert "APSW version" in captured.out


def test_get_parser_help(capsys):
    parser = argparse_config.get_parser("1.0.0")
    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(["--help"])
    assert excinfo.value.code == 0
    captured = capsys.readouterr()
    assert "[--report-genome-build [yes/no]]" in captured.out


def test_get_parser_report_configuration():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--report-configuration", "yes"])
    assert args.report_configuration == "yes"


def test_get_parser_report_replications_fingerprint():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--report-replication-fingerprint", "no"])
    assert args.report_replication_fingerprint == "no"


def test_get_parser_random_number_generator_seed():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--random-number-generator-seed", "12345"])
    assert args.random_number_generator_seed == "12345"


def test_get_parser_knowledge():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--knowledge", "knowledge.db"])
    assert args.knowledge == "knowledge.db"


def test_get_parser_report_genome_build():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--report-genome-build", "yes"])
    assert args.report_genome_build == "yes"


def test_get_parser_report_gene_name_stats():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--report-gene-name-stats", "no"])
    assert args.report_gene_name_stats == "no"


def test_get_parser_report_group_name_stats():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--report-group-name-stats", "yes"])
    assert args.report_group_name_stats == "yes"


def test_get_parser_allow_unvalidated_snp_positions():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--allow-unvalidated-snp-positions", "yes"])
    assert args.allow_unvalidated_snp_positions == "yes"


def test_get_parser_allow_ambiguous_snps():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--allow-ambiguous-snps", "no"])
    assert args.allow_ambiguous_snps == "no"


def test_get_parser_allow_ambiguous_knowledge():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--allow-ambiguous-knowledge", "no"])
    assert args.allow_ambiguous_knowledge == "no"


def test_get_parser_reduce_ambiguous_knowledge():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--reduce-ambiguous-knowledge", "any"])
    assert args.reduce_ambiguous_knowledge == "any"


def test_get_parser_report_ld_profiles():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--report-ld-profiles", "no"])
    assert args.report_ld_profiles == "no"


def test_get_parser_ld_profile():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--ld-profile", "profile1"])
    assert args.ld_profile == "profile1"


def test_get_parser_verify_biofilter_version():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--verify-biofilter-version", "1.0.0"])
    assert args.verify_biofilter_version == "1.0.0"


def test_get_parser_verify_loki_version():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--verify-loki-version", "2.0.0"])
    assert args.verify_loki_version == "2.0.0"


def test_get_parser_verify_source_loader():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--verify-source-loader", "source1", "1.0.0"])
    assert args.verify_source_loader == [["source1", "1.0.0"]]


def test_get_parser_verify_source_option():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(
        ["--verify-source-option", "source1", "option1", "value1"]
    )  # noqa E501
    assert args.verify_source_option == [["source1", "option1", "value1"]]


def test_get_parser_verify_source_file():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(
        ["--verify-source-file", "source1", "file1", "2023-01-01", "1000", "md5"]
    )  # noqa E501
    assert args.verify_source_file == [
        ["source1", "file1", "2023-01-01", "1000", "md5"]
    ]  # noqa E501


def test_get_parser_user_defined_knowledge():
    parser = argparse_config.get_parser("1.0.0")
    args = parser.parse_args(["--user-defined-knowledge", "udk1"])
    assert args.user_defined_knowledge == ["udk1"]


# ###############################################################
# TODO: Split the following tests into separate test units
# ###############################################################


def test_get_parser():
    parser = argparse_config.get_parser("1.0.0")

    args = parser.parse_args(["--user-defined-filter", "group"])
    assert args.user_defined_filter == "group"

    args = parser.parse_args(["--snp", "rs123"])
    assert args.snp == [["rs123"]]

    args = parser.parse_args(["--snp-file", "snp_file.txt"])
    assert args.snp_file == [["snp_file.txt"]]

    args = parser.parse_args(["--position", "chr1:1000"])
    assert args.position == [["chr1:1000"]]

    args = parser.parse_args(["--position-file", "position_file.txt"])
    assert args.position_file == [["position_file.txt"]]

    args = parser.parse_args(["--gene", "gene1"])
    assert args.gene == [["gene1"]]

    args = parser.parse_args(["--gene-file", "gene_file.txt"])
    assert args.gene_file == [["gene_file.txt"]]

    args = parser.parse_args(["--gene-identifier-type", "type1"])
    assert args.gene_identifier_type == "type1"

    args = parser.parse_args(["--allow-ambiguous-genes", "no"])
    assert args.allow_ambiguous_genes == "no"

    args = parser.parse_args(["--gene-search", "search1"])
    assert args.gene_search == [["search1"]]

    args = parser.parse_args(["--region", "chr1:1000-2000"])
    assert args.region == [["chr1:1000-2000"]]

    args = parser.parse_args(["--region-file", "region_file.txt"])
    assert args.region_file == [["region_file.txt"]]

    args = parser.parse_args(["--group", "group1"])
    assert args.group == [["group1"]]

    args = parser.parse_args(["--group-file", "group_file.txt"])
    assert args.group_file == [["group_file.txt"]]

    args = parser.parse_args(["--group-identifier-type", "type1"])
    assert args.group_identifier_type == "type1"

    args = parser.parse_args(["--allow-ambiguous-groups", "no"])
    assert args.allow_ambiguous_groups == "no"

    args = parser.parse_args(["--group-search", "search1"])
    assert args.group_search == [["search1"]]

    args = parser.parse_args(["--source", "source1"])
    assert args.source == [["source1"]]

    args = parser.parse_args(["--source-file", "source_file.txt"])
    assert args.source_file == [["source_file.txt"]]

    args = parser.parse_args(["--alt-snp", "rs123"])
    assert args.alt_snp == [["rs123"]]

    args = parser.parse_args(["--alt-snp-file", "alt_snp_file.txt"])
    assert args.alt_snp_file == [["alt_snp_file.txt"]]

    args = parser.parse_args(["--alt-position", "chr1:1000"])
    assert args.alt_position == [["chr1:1000"]]

    args = parser.parse_args(["--alt-position-file", "alt_position_file.txt"])
    assert args.alt_position_file == [["alt_position_file.txt"]]

    args = parser.parse_args(["--alt-gene", "gene1"])
    assert args.alt_gene == [["gene1"]]

    args = parser.parse_args(["--alt-gene-file", "alt_gene_file.txt"])
    assert args.alt_gene_file == [["alt_gene_file.txt"]]

    args = parser.parse_args(["--alt-gene-search", "search1"])
    assert args.alt_gene_search == [["search1"]]

    args = parser.parse_args(["--alt-region", "chr1:1000-2000"])
    assert args.alt_region == [["chr1:1000-2000"]]

    args = parser.parse_args(["--alt-region-file", "alt_region_file.txt"])
    assert args.alt_region_file == [["alt_region_file.txt"]]

    args = parser.parse_args(["--alt-group", "group1"])
    assert args.alt_group == [["group1"]]

    args = parser.parse_args(["--alt-group-file", "alt_group_file.txt"])
    assert args.alt_group_file == [["alt_group_file.txt"]]

    args = parser.parse_args(["--alt-group-search", "search1"])
    assert args.alt_group_search == [["search1"]]

    args = parser.parse_args(["--alt-source", "source1"])
    assert args.alt_source == [["source1"]]

    args = parser.parse_args(["--alt-source-file", "alt_source_file.txt"])
    assert args.alt_source_file == [["alt_source_file.txt"]]

    args = parser.parse_args(["--grch-build-version", "38"])
    assert args.grch_build_version == 38

    args = parser.parse_args(["--ucsc-build-version", "19"])
    assert args.ucsc_build_version == 19

    args = parser.parse_args(["--coordinate-base", "0"])
    assert args.coordinate_base == 0

    args = parser.parse_args(["--regions-half-open", "yes"])
    assert args.regions_half_open == "yes"

    args = parser.parse_args(["--region-position-margin", "1000"])
    assert args.region_position_margin == 1000

    args = parser.parse_args(["--region-match-percent", "50%"])
    assert args.region_match_percent == 50

    args = parser.parse_args(["--region-match-bases", "1000"])
    assert args.region_match_bases == 1000

    args = parser.parse_args(["--maximum-model-count", "10"])
    assert args.maximum_model_count == 10

    args = parser.parse_args(["--alternate-model-filtering", "yes"])
    assert args.alternate_model_filtering == "yes"

    args = parser.parse_args(["--all-pairwise-models", "no"])
    assert args.all_pairwise_models == "no"

    args = parser.parse_args(["--maximum-model-group-size", "20"])
    assert args.maximum_model_group_size == 20

    args = parser.parse_args(["--minimum-model-score", "5"])
    assert args.minimum_model_score == 5

    args = parser.parse_args(["--sort-models", "yes"])
    assert args.sort_models == "yes"

    args = parser.parse_args(["--paris-p-value", "0.01"])
    assert args.paris_p_value == 0.01

    args = parser.parse_args(["--paris-zero-p-values", "sig"])
    assert args.paris_zero_p_values == "significant"

    args = parser.parse_args(["--paris-max-p-value", "0.1"])
    assert args.paris_max_p_value == 0.1

    args = parser.parse_args(["--paris-enforce-input-chromosome", "yes"])
    assert args.paris_enforce_input_chromosome == "yes"

    args = parser.parse_args(["--paris-permutation-count", "500"])
    assert args.paris_permutation_count == 500

    args = parser.parse_args(["--paris-bin-size", "5000"])
    assert args.paris_bin_size == 5000

    args = parser.parse_args(["--paris-snp-file", "paris_snp_file.txt"])
    assert args.paris_snp_file == [["paris_snp_file.txt"]]

    args = parser.parse_args(
        ["--paris-position-file", "paris_position_file.txt"]
    )  # noqa E501
    assert args.paris_position_file == [["paris_position_file.txt"]]

    args = parser.parse_args(["--paris-details", "yes"])
    assert args.paris_details == "yes"

    args = parser.parse_args(["--quiet", "yes"])
    assert args.quiet == "yes"

    args = parser.parse_args(["--verbose", "no"])
    assert args.verbose == "no"

    args = parser.parse_args(["--prefix", "output"])
    assert args.prefix == "output"

    args = parser.parse_args(["--overwrite", "yes"])
    assert args.overwrite == "yes"

    args = parser.parse_args(["--stdout", "no"])
    assert args.stdout == "no"

    args = parser.parse_args(["--report-invalid-input", "yes"])
    assert args.report_invalid_input == "yes"

    args = parser.parse_args(["--filter", "type1"])
    assert args.filter == [["type1"]]

    args = parser.parse_args(["--annotate", "type1"])
    assert args.annotate == [["type1"]]

    args = parser.parse_args(["--model", "type1"])
    assert args.model == [["type1"]]

    args = parser.parse_args(["--paris", "yes"])
    assert args.paris == "yes"

    args = parser.parse_args(["--end-of-line"])
    assert args.end_of_line is True

    args = parser.parse_args(["--allow-duplicate-output", "yes"])
    assert args.allow_duplicate_output == "yes"

    args = parser.parse_args(["--debug-logic"])
    assert args.debug_logic is True

    args = parser.parse_args(["--debug-query"])
    assert args.debug_query is True

    args = parser.parse_args(["--debug-profile"])
    assert args.debug_profile is True
