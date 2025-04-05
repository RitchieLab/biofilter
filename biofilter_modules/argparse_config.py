"""
argparse_config.py

This module defines the argument parsing configuration and helper functions
for the Biofilter application.
It leverages Python's argparse library to set up and organize command-line
options, enabling users to customize the application's behavior through
various parameters and flags.

Key Components:
- Argument Parsing: Defines various argument groups and options, organized
    into sections for ease of use. Supports general configuration, output
    settings, input data formats, filtering options, and more.
- Custom Type Handlers: Includes custom handler functions for specialized
    argument parsing needs, such as handling percentage values, boolean-like
    inputs, and ranges within specified bounds.
- Configuration File Parsing: Supports the loading of options from
    configuration files using a CSV dialect, allowing users to manage complex
    sets of options in an organized manner.

Usage:
This module is intended to be imported and utilized by the main Biofilter
script. To get the argument parser, use `get_parser(version)`, where `version`
is the current version of the Biofilter application.

Example:
    parser = get_parser("1.0.0")
    args = parser.parse_args()
    # Use parsed arguments in the application

Functions:
- get_parser(version): Returns a configured ArgumentParser instance with all
    options set up for Biofilter.
- parseCFile(cfName, parser, options): Recursively parses a configuration file
    for options, updating the provided parser and options namespace with
    values from the file.

This module allows for a flexible, organized way to handle command-line and
    configuration file options, enhancing user control over the Biofilter
    application.
"""

import argparse

from loki_modules import loki_db  # noqa E402


def yesno(val):
    """
    Define custom bool-ish type handler
    """
    val = str(val).strip().lower()
    if val in ("1", "t", "true", "y", "yes", "on"):
        return "yes"
    if val in ("0", "f", "false", "n", "no", "off"):
        return "no"
    raise argparse.ArgumentTypeError(
        "'%s' must be yes/on/true/1 or no/off/false/0" % val
    )


def percent(val):
    """
    Define custom percentage type handler
    """
    val = str(val).strip().lower()
    while val.endswith("%"):
        val = val[:-1]
    val = float(val)
    if val > 100:
        raise argparse.ArgumentTypeError("'%s' must be <= 100" % val)
    return val


def zerotoone(val):
    """
    Validates that a given value is a float between 0.0 and 1.0, inclusive.
    """
    val = float(val)
    if val < 0.0 or val > 1.0:
        raise argparse.ArgumentTypeError(
            "'%s' must be between 0.0 and 1.0" % (val,)
        )  # noqa: E501
    return val


def basepairs(val):
    """
    Converts a string representing base pairs with units (k, m, g) into an
    integer.

    Interprets suffixes 'k', 'm', and 'g' as multipliers for thousands,
    millions, and billions, respectively, and removes a final 'b' suffix if
    present.

    Args:
        val (str): A string representing a number of base pairs, with optional
        suffix.

    Returns:
        int: The equivalent integer value of base pairs after conversion.
    """
    val = str(val).strip().lower()
    if val[-1:] == "b":
        val = val[:-1]
    if val[-1:] == "k":
        val = int(val[:-1]) * 1000
    elif val[-1:] == "m":
        val = int(val[:-1]) * 1000 * 1000
    elif val[-1:] == "g":
        val = int(val[:-1]) * 1000 * 1000 * 1000
    else:
        val = int(val)
    return val


def typePZPV(val):
    """
    Converts a given value to one of the predefined strings: "significant",
    "insignificant", or "ignore".

    Parameters: val (str):
        The input value to be converted.

    Returns: (str)
        The converted value which can be "significant", "insignificant", or
        "ignore".

    Raises:
        argparse.ArgumentTypeError: If the input value is ambiguous or does
        not match any of the predefined strings.
    """
    val = str(val).strip().lower()
    if "significant".startswith(val):
        return "significant"
    if val == "i":
        raise argparse.ArgumentTypeError(
            "ambiguous value: '%s' could match insignificant, ignore" % (val,)
        )
    if "insignificant".startswith(val):
        return "insignificant"
    if "ignore".startswith(val):
        return "ignore"
    raise argparse.ArgumentTypeError(
        "'%s' must be significant, insignificant or ignore" % (val,)
    )


def get_parser(version):
    """
    Creates and configures an ArgumentParser for the Biofilter application,
    defining all the command-line options and flags that control the
    application's behavior.

    Parameters:
    - version (str): The version string of the Biofilter application, included
    in the help and version outputs.

    Returns:
    - argparse.ArgumentParser: A configured ArgumentParser instance with all
    command-line options defined, organized into sections for easy navigation
    and usage.

    Details:
    - This function sets up argument groups and custom types to handle
        Biofilter's unique input needs.
    - It includes options for general configuration, file paths, logging,
        filtering, and data management.
    - Custom type handlers (like `yesno`, `percent`, and `zerotoone`) ensure
        valid input for specific arguments.
    - Provides support for configuration file parsing by allowing options to
        be loaded and overridden from external files.

    Example:
        parser = get_parser("1.0.0")
        args = parser.parse_args()
        # Use `args` to access command-line arguments
    """

    parser = argparse.ArgumentParser(
        description=version,
        add_help=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # add general configuration section
    group = parser.add_argument_group("Configuration Options")
    group.add_argument(
        "--help", "-h", action="help", help="show this help message and exit"
    )
    group.add_argument(
        "--version",
        action="version",
        help="show all software version numbers and exit",
        version=version
        + """
        %9s version %s
        %9s version %s
        %9s version %s
        """
        % (
            "LOKI",
            loki_biofilter.db.Database.getVersionString(),
            loki_biofilter.db.Database.getDatabaseDriverName(),
            loki_biofilter.db.Database.getDatabaseDriverVersion(),
            loki_biofilter.db.Database.getDatabaseInterfaceName(),
            loki_biofilter.db.Database.getDatabaseInterfaceVersion(),
        ),
    )
    group.add_argument(
        "configuration",
        type=str,
        metavar="configuration_file",
        nargs="*",
        default=None,
        help="a file from which to read additional options",
    )
    group.add_argument(
        "--report-configuration",
        "--rc",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help=(
            "output a report of all effective options, including any "
            "defaults, in a configuration file format which can be re-input "
            "(default: no)"
        ),
    )
    group.add_argument(
        "--report-replication-fingerprint",
        "--rrf",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help=(
            "include software versions and the knowledge database file's "
            "fingerprint values in the configuration report, to ensure the "
            "same data is used in replication (default: no)"
        ),
    )
    group.add_argument(
        "--random-number-generator-seed",
        "--rngs",
        type=str,
        metavar="seed",
        nargs="?",
        const="",
        default=None,
        help="seed value for the PRNG, or blank to use the sytem default (default: blank)",  # noqa: E501
    )

    # add knowledge database section
    group = parser.add_argument_group("Prior Knowledge Options")
    group.add_argument(
        "--knowledge",
        "-k",
        type=str,
        metavar="file",  # default=argparse.SUPPRESS,
        help="the prior knowledge database file to use",
    )
    group.add_argument(
        "--report-genome-build",
        "--rgb",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="yes",
        help="report the genome build version number used by the knowledge database (default: yes)",  # noqa: E501
    )
    group.add_argument(
        "--report-gene-name-stats",
        "--rgns",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="display statistics on available gene identifier types (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--report-group-name-stats",
        "--runs",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="display statistics on available group identifier types (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--allow-unvalidated-snp-positions",
        "--ausp",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="yes",
        help="use unvalidated SNP positions in the knowledge database (default: yes)",  # noqa: E501
    )
    group.add_argument(
        "--allow-ambiguous-snps",
        "--aas",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="use SNPs which have ambiguous loci in the knowledge database (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--allow-ambiguous-knowledge",
        "--aak",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="use ambiguous group<->gene associations in the knowledge database (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--reduce-ambiguous-knowledge",
        "--rak",
        type=str,
        metavar="no/implication/quality/any",
        nargs="?",
        const="any",
        default="no",
        choices=["no", "implication", "quality", "any"],
        help=(
            "attempt to reduce ambiguity in the knowledge database using a "
            "heuristic strategy, from 'no', 'implication', 'quality' or 'any' "
            "(default: no)"
        ),
    )
    group.add_argument(
        "--report-ld-profiles",
        "--rlp",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="display the available LD profiles and their properties (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--ld-profile",
        "--lp",
        type=str,
        metavar="profile",
        nargs="?",
        const=None,
        default=None,
        help="LD profile with which to adjust regions in the knowledge database (default: none)",  # noqa: E501
    )
    group.add_argument(
        "--verify-biofilter-version",
        type=str,
        metavar="version",
        default=None,
        help="require a specific Biofilter software version to replicate results",  # noqa: E501
    )
    group.add_argument(
        "--verify-loki-version",
        type=str,
        metavar="version",
        default=None,
        help="require a specific LOKI software version to replicate results",
    )
    group.add_argument(
        "--verify-source-loader",
        type=str,
        metavar=("source", "version"),
        nargs=2,
        action="append",
        default=None,
        help="require that the knowledge database was built with a specific source loader version",  # noqa: E501
    )
    group.add_argument(
        "--verify-source-option",
        type=str,
        metavar=("source", "option", "value"),
        nargs=3,
        action="append",
        default=None,
        help="require that the knowledge database was built with a specific source loader option",  # noqa: E501
    )
    group.add_argument(
        "--verify-source-file",
        type=str,
        metavar=("source", "file", "date", "size", "md5"),
        nargs=5,
        action="append",
        default=None,
        help="require that the knowledge database was built with a specific source file fingerprint",  # noqa: E501
    )
    group.add_argument(
        "--user-defined-knowledge",
        "--udk",
        type=str,
        metavar="file",
        nargs="+",
        default=None,
        help="file(s) from which to load user-defined knowledge",
    )
    group.add_argument(
        "--user-defined-filter",
        "--udf",
        type=str,
        metavar="no/group/gene",
        default="no",
        choices=["no", "group", "gene"],
        help=(
            "method by which user-defined knowledge will also be applied as a "
            "filter on other prior knowledge, from 'no', 'group' or 'gene' "
            "(default: no)"
        ),
    )

    # add primary input section
    group = parser.add_argument_group("Input Data Options")
    group.add_argument(
        "--snp",
        "-s",
        type=str,
        metavar="rs#",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="input SNPs, specified by RS#",
    )
    group.add_argument(
        "--snp-file",
        "-S",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load input SNPs",
    )
    group.add_argument(
        "--position",
        "-p",
        type=str,
        metavar="position",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="input positions, specified by chromosome and basepair coordinate",  # noqa: E501
    )
    group.add_argument(
        "--position-file",
        "-P",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load input positions",
    )
    group.add_argument(
        "--gene",
        "-g",
        type=str,
        metavar="name",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="input genes, specified by name",
    )
    group.add_argument(
        "--gene-file",
        "-G",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load input genes",
    )
    group.add_argument(
        "--gene-identifier-type",
        "--git",
        type=str,
        metavar="type",
        nargs="?",
        const="*",
        default="-",
        help=(
            "the default type of any gene identifiers without types, or a "
            "special type '=', '-' or '*' (default: '-' for primary labels)"
        ),
    )
    group.add_argument(
        "--allow-ambiguous-genes",
        "--aag",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="use ambiguous input gene identifiers by including all possibilities (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--gene-search",
        "--gs",
        type=str,
        metavar="text",
        nargs="+",
        action="append",
        help="find input genes by searching all available names and descriptions",  # noqa: E501
    )
    group.add_argument(
        "--region",
        "-r",
        type=str,
        metavar="region",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="input regions, specified by chromosome, start and stop positions",  # noqa: E501
    )
    group.add_argument(
        "--region-file",
        "-R",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load input regions",
    )
    group.add_argument(
        "--group",
        "-u",
        type=str,
        metavar="name",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="input groups, specified by name",
    )
    group.add_argument(
        "--group-file",
        "-U",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load input groups",
    )
    group.add_argument(
        "--group-identifier-type",
        "--uit",
        type=str,
        metavar="type",
        nargs="?",
        const="*",
        default="-",
        help=(
            "the default type of any group identifiers without types, or a "
            "special type '=', '-' or '*' (default: '-' for primary labels)"
        ),
    )
    group.add_argument(
        "--allow-ambiguous-groups",
        "--aau",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="use ambiguous input group identifiers by including all possibilities (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--group-search",
        "--us",
        type=str,
        metavar="text",
        nargs="+",
        action="append",
        help="find input groups by searching all available names and descriptions",  # noqa: E501
    )
    group.add_argument(
        "--source",
        "-c",
        type=str,
        metavar="name",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="input sources, specified by name",
    )
    group.add_argument(
        "--source-file",
        "-C",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load input sources",
    )

    # add alternate input section
    group = parser.add_argument_group("Alternate Input Data Options")
    group.add_argument(
        "--alt-snp",
        "--as",
        type=str,
        metavar="rs#",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="alternate input SNPs, specified by RS#",
    )
    group.add_argument(
        "--alt-snp-file",
        "--AS",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load alternate input SNPs",
    )
    group.add_argument(
        "--alt-position",
        "--ap",
        type=str,
        metavar="position",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="alternate input positions, specified by chromosome and basepair coordinate",  # noqa: E501
    )
    group.add_argument(
        "--alt-position-file",
        "--AP",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load alternate input positions",
    )
    group.add_argument(
        "--alt-gene",
        "--ag",
        type=str,
        metavar="name",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="alternate input genes, specified by name",
    )
    group.add_argument(
        "--alt-gene-file",
        "--AG",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load alternate input genes",
    )
    group.add_argument(
        "--alt-gene-search",
        "--ags",
        type=str,
        metavar="text",
        nargs="+",
        action="append",
        help="find alternate input genes by searching all available names and descriptions",  # noqa: E501
    )
    group.add_argument(
        "--alt-region",
        "--ar",
        type=str,
        metavar="region",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="alternate input regions, specified by chromosome, start and stop positions",  # noqa: E501
    )
    group.add_argument(
        "--alt-region-file",
        "--AR",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load alternate input regions",
    )
    group.add_argument(
        "--alt-group",
        "--au",
        type=str,
        metavar="name",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="alternate input groups, specified by name",
    )
    group.add_argument(
        "--alt-group-file",
        "--AU",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load alternate input groups",
    )
    group.add_argument(
        "--alt-group-search",
        "--aus",
        type=str,
        metavar="text",
        nargs="+",
        action="append",
        help="find alternate input groups by searching all available names and descriptions",  # noqa: E501
    )
    group.add_argument(
        "--alt-source",
        "--ac",
        type=str,
        metavar="name",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="alternate input sources, specified by name",
    )
    group.add_argument(
        "--alt-source-file",
        "--AC",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load alternate input sources",
    )

    # add positional section
    group = parser.add_argument_group("Positional Matching Options")
    group.add_argument(
        "--grch-build-version",
        "--gbv",
        type=int,
        metavar="version",
        default=None,
        help="the GRCh# human reference genome build version of position and region inputs",  # noqa: E501
    )
    group.add_argument(
        "--ucsc-build-version",
        "--ubv",
        type=int,
        metavar="version",
        default=None,
        help="the UCSC hg# human reference genome build version of position and region inputs",  # noqa: E501
    )
    group.add_argument(
        "--coordinate-base",
        "--cb",
        type=int,
        metavar="offset",
        default=1,
        help="the coordinate base for position and region inputs and outputs (default: 1)",  # noqa: E501
    )
    group.add_argument(
        "--regions-half-open",
        "--rho",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help=(
            "whether input and output regions are 'half-open' intervals and "
            "should not include their end coordinate (default: no)"
        ),
    )
    group.add_argument(
        "--region-position-margin",
        "--rpm",
        type=basepairs,
        metavar="bases",
        default=0,
        help=(
            "number of bases beyond the bounds of known regions where "
            "positions should still be matched (default: 0)"
        ),
    )
    group.add_argument(
        "--region-match-percent",
        "--rmp",
        type=percent,
        metavar="percentage",
        default=None,  # default set later, with -bases
        help=(
            "minimum percentage of overlap between two regions to consider "
            "them a match (default: 100)"
        ),
    )
    group.add_argument(
        "--region-match-bases",
        "--rmb",
        type=basepairs,
        metavar="bases",
        default=None,  # default set later, with -percent
        help=(
            "minimum number of bases of overlap between two regions to "
            "consider them a match (default: 0)"
        ),
    )

    # add modeling section
    group = parser.add_argument_group("Model-Building Options")
    group.add_argument(
        "--maximum-model-count",
        "--mmc",
        type=int,
        metavar="count",
        nargs="?",
        const=0,
        default=0,
        help="maximum number of models to generate, or < 1 for unlimited (default: unlimited)",  # noqa: E501
    )
    group.add_argument(
        "--alternate-model-filtering",
        "--amf",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="apply primary input filters to only one side of generated models (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--all-pairwise-models",
        "--apm",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="generate all comprehensive pairwise models without regard to any prior knowledge (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--maximum-model-group-size",
        "--mmgs",
        type=int,
        metavar="size",
        default=30,
        help="maximum size of a group to use for knowledge-supported models, or < 1 for unlimited (default: 30)",  # noqa: E501
    )
    group.add_argument(
        "--minimum-model-score",
        "--mms",
        type=int,
        metavar="score",
        default=2,
        help="minimum implication score for knowledge-supported models (default: 2)",  # noqa: E501
    )
    group.add_argument(
        "--sort-models",
        "--sm",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="yes",
        help="output knowledge-supported models in order of descending score (default: yes)",  # noqa: E501
    )

    # add PARIS section
    group = parser.add_argument_group("PARIS Options")
    group.add_argument(
        "--paris-p-value",
        "--ppv",
        type=zerotoone,
        metavar="p-value",
        default=0.05,
        help="maximum p-value of input results to be considered significant (default: 0.05)",  # noqa: E501
    )
    group.add_argument(
        "--paris-zero-p-values",
        "--pzpv",
        type=typePZPV,
        metavar="sig/insig/ignore",
        default="ignore",
        help="how to consider input result p-values of zero (default: ignore)",
    )
    group.add_argument(
        "--paris-max-p-value",
        "--pmpv",
        type=zerotoone,
        metavar="p-value",
        default=None,
        help="maximum meaningful permutation p-value (default: none)",
    )
    group.add_argument(
        "--paris-enforce-input-chromosome",
        "--peic",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="yes",
        help="limit input result SNPs to positions on the specified chromosome (default: yes)",  # noqa: E501
    )
    group.add_argument(
        "--paris-permutation-count",
        "--ppc",
        type=int,
        metavar="number",
        default=1000,
        help="number of permutations to perform on each group and gene (default: 1000)",  # noqa: E501
    )
    group.add_argument(
        "--paris-bin-size",
        "--pbs",
        type=int,
        metavar="number",
        default=10000,
        help="ideal number of features per bin (default: 10000)",
    )
    group.add_argument(
        "--paris-snp-file",
        "--PS",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load SNP results",
    )
    group.add_argument(
        "--paris-position-file",
        "--PP",
        type=str,
        metavar="file",
        nargs="+",
        action="append",  # default=argparse.SUPPRESS,
        help="file(s) from which to load position results",
    )
    group.add_argument(
        "--paris-details",
        "--pd",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="generate the PARIS detail report (default: no)",
    )

    # add output section
    group = parser.add_argument_group("Output Options")
    group.add_argument(
        "--quiet",
        "-q",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="don't print any warnings or log messages to <stdout> (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--verbose",
        "-v",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="print additional informational log messages to <stdout> (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--prefix",
        type=str,
        metavar="prefix",
        default="biofilter",
        help=(
            "prefix to use for all output filenames; may contain path "
            "components (default: 'biofilter')"
        ),
    )
    group.add_argument(
        "--overwrite",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="overwrite any existing output files (default: no)",
    )
    group.add_argument(
        "--stdout",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help=(
            "display all output data directly on <stdout> rather than writing "
            "to any files (default: no)"
        ),
    )
    group.add_argument(
        "--report-invalid-input",
        "--rii",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="report invalid input data lines in a separate output file for each type (default: no)",  # noqa: E501
    )
    group.add_argument(
        "--filter",
        "-f",
        type=str,
        metavar="type",
        nargs="+",
        action="append",
        help="data types or columns to include in the filtered output",
    )
    group.add_argument(
        "--annotate",
        "-a",
        type=str,
        metavar="type",
        nargs="+",
        action="append",
        help="data types or columns to include in the annotated output",
    )
    group.add_argument(
        "--model",
        "-m",
        type=str,
        metavar="type",
        nargs="+",
        action="append",
        help="data types or columns to include in the output models",
    )
    group.add_argument(
        "--paris",
        type=str,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help="perform a PARIS analysis with the provided input data (default: no)",  # noqa: E501
    )

    # add hidden options
    parser.add_argument(
        "--end-of-line", action="store_true", help=argparse.SUPPRESS
    )  # noqa: E501
    parser.add_argument(
        "--allow-duplicate-output",
        "--ado",
        type=yesno,
        metavar="yes/no",
        nargs="?",
        const="yes",
        default="no",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--debug-logic", action="store_true", help=argparse.SUPPRESS
    )  # noqa: E501
    parser.add_argument(
        "--debug-query", action="store_true", help=argparse.SUPPRESS
    )  # noqa: E501
    parser.add_argument(
        "--debug-profile", action="store_true", help=argparse.SUPPRESS
    )  # noqa: E501

    return parser
