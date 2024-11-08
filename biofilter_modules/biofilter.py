#!/usr/bin/env python

import codecs
import collections
import itertools
import os
import random

import sys
import time

from biofilter_class import Biofilter
from argparse_config import get_parser
from arg_utils import OrderedNamespace
from arg_utils import parseCFile

from loki_modules import loki_db  # noqa E402


def main():
    # Factory function that returns a logging lambda for the specified module
    def cbMake(modtype):
        return lambda line, err: cbLog[modtype].extend(
            [
                # Error message, defaulting to '(unknown error)'
                f"# {err or '(unknown error)'}",
                # Line where the error occurred, with trailing wp removed
                str(line).rstrip(),
            ]
        )

    def encodeString(string):
        return utf8(string)[0]

    def encodeLine(line, term="\n"):
        return utf8("%s%s" % (line, term))[0]

    def encodeRow(row, term="\n", delim="\t"):
        return utf8(
            "%s%s"
            % (
                (
                    delim.join(
                        (
                            col
                            if isinstance(col, str)
                            else str("" if col is None else col)  # noqa E501
                        )  # noqa E501
                        for col in row
                    )
                ),
                term,
            )
        )[0]

    version = "Biofilter version %s" % (Biofilter.getVersionString())

    # Initialize the argument parser with predefined configuration options and
    # help text. The 'get_parser' function provides a parser configured for
    # this application version.
    parser = get_parser(version)

    # if there are no arguments, just print usage and exit
    if len(sys.argv) < 2:
        print(version)
        print
        parser.print_usage()
        print
        print("Use -h for details.")
        sys.exit(2)

    # Initialize 'options' with default argument values using an empty
    # argument list and a custom OrderedNamespace to store arguments in the
    # order they are added. This setup allows additional programmatic
    # manipulation before processing user input.
    options = parser.parse_args(args=[], namespace=OrderedNamespace())

    # Parse command-line arguments once to ensure all options and custom types
    # (e.g., `yesno`) are processed. Then, iterate through configuration files,
    # if any, using the parsed arguments.
    for cfName in parser.parse_args().configuration:
        # Parse configuration files and override command-line options if needed
        parseCFile(cfName, parser, options)

    # Parse command-line arguments into the existing `options` object, allowing
    # command-line arguments to override settings from configuration files.
    parser.parse_args(namespace=options)

    # Instantiate the Biofilter class with the parsed options. This step
    # initializes the Biofilter object with the provided configuration.
    bio = Biofilter(options)

    # Define an empty list for use in filtering and annotation operations.
    empty = list()

    # identify all the reports we need to output
    typeOutputPath = collections.OrderedDict()
    typeOutputPath["report"] = collections.OrderedDict()

    if options.report_configuration == "yes":
        typeOutputPath["report"]["configuration"] = (
            options.prefix + ".configuration"
        )  # noqa E501
    if options.report_gene_name_stats == "yes":
        typeOutputPath["report"]["gene name statistics"] = (
            options.prefix + ".gene-names"
        )
    if options.report_group_name_stats == "yes":
        typeOutputPath["report"]["group name statistics"] = (
            options.prefix + ".group-names"
        )
    if options.report_ld_profiles == "yes":
        typeOutputPath["report"]["LD profiles"] = (
            options.prefix + ".ld-profiles"
        )  # noqa E501

    # define invalid input handlers, if requested
    typeOutputPath["invalid"] = collections.OrderedDict()
    cb = collections.defaultdict(bool)
    cbLog = collections.OrderedDict()

    if options.report_invalid_input == "yes":
        for itype in ["SNP", "position", "region", "gene", "group", "source"]:
            for mod in ["", "alt-"]:
                typeOutputPath["invalid"][mod + itype] = (
                    options.prefix + ".invalid." + mod + itype.lower()
                )
                cbLog[mod + itype] = list()
        for itype in ["userknowledge"]:
            typeOutputPath["invalid"][itype] = (
                options.prefix + ".invalid." + itype.lower()
            )
            cbLog[itype] = list()
    # if report invalid input

    # identify all the filtering results we need to output
    typeOutputPath["filter"] = collections.OrderedDict()
    for types in options.filter or empty:
        if types:
            typeOutputPath["filter"][tuple(types)] = (
                options.prefix + "." + "-".join(types)
            )
        else:
            # ignore empty filters
            pass
    # foreach requested filter

    # identify all the annotation results we need to output
    typeOutputPath["annotation"] = collections.OrderedDict()

    if options.snp or options.snp_file:
        userInputType = ["snpinput"]
    elif options.position_file or options.position:
        userInputType = ["positioninput"]
    elif options.gene or options.gene_file or options.gene_search:
        userInputType = ["geneinput"]
    elif options.region or options.region_file:
        userInputType = ["regioninput"]
    elif options.group or options.group_file or options.group_search:
        userInputType = ["groupinput"]
    elif options.source or options.source_file:
        userInputType = ["sourceinput"]
    else:
        userInputType = []

    for types in options.annotate or empty:
        n = types.count(":")
        if n > 1:
            sys.exit(
                "ERROR: cannot annotate '%s', only two sets of outputs are allowed\n"  # noqa E501
                % (" ".join(types),)
            )
        elif n:
            i = types.index(":")
            typesF = userInputType + types[:i]
            typesA = types[i + 1 : None]  # noqa E203
        else:
            typesF = userInputType + types[0:1]
            typesA = types[1:None]

        if typesF and typesA:
            typeOutputPath["annotation"][(tuple(typesF), tuple(typesA))] = (
                options.prefix
                + "."
                + "-".join(typesF[1:])
                + "."
                + "-".join(typesA)  # noqa E501
            )
        elif typesF:
            bio.warn(
                "WARNING: annotating '%s' is equivalent to filtering '%s'\n"
                % (" ".join(types), " ".join(typesF))
            )
            typeOutputPath["filter"][tuple(typesF)] = (
                options.prefix + "." + "-".join(typesF)
            )
        elif typesA:
            sys.exit(
                "ERROR: cannot annotate '%s' with no starting point\n"
                % (" ".join(types),)
            )
        else:
            # ignore empty annotations
            pass
    # foreach requested annotation

    # identify all the model results we need to output
    typeOutputPath["models"] = collections.OrderedDict()
    for types in options.model or empty:
        n = types.count(":")
        if n > 1:
            sys.exit(
                "ERROR: cannot model '%s', only two sets of outputs are allowed\n"  # noqa E501
                % (" ".join(types),)
            )
        elif n:
            i = types.index(":")
            typesL = types[:i]
            typesR = types[i + 1 : None]  # noqa E203
        else:
            typesL = typesR = types

        if not (typesL or typesR):
            # ignore empty models
            pass
        elif not (typesL and typesR):
            sys.exit(
                "ERROR: cannot model '%s', both sides require at least one output type\n"  # noqa E501
                % " ".join(types)
            )
        elif typesL == typesR:
            typeOutputPath["models"][(tuple(typesL), tuple(typesR))] = (
                options.prefix + "." + "-".join(typesL) + ".models"
            )
        else:
            typeOutputPath["models"][(tuple(typesL), tuple(typesR))] = (
                options.prefix
                + "."
                + "-".join(typesL)
                + "."
                + "-".join(typesR)
                + ".models"
            )
    # foreach requested model

    # identify all the PARIS result files we need to output
    typeOutputPath["paris"] = collections.OrderedDict()
    if options.paris == "yes":
        typeOutputPath["paris"]["summary"] = options.prefix + ".paris-summary"
        if options.paris_details == "yes":
            typeOutputPath["paris"]["detail"] = (
                options.prefix + ".paris-detail"
            )  # noqa E501

    # verify that all output files are unique, writeable and nonexistant
    # (unless overwriting)
    typeOutputInfo = dict()
    pathUsed = dict()
    for outtype, outputPath in typeOutputPath.items():
        typeOutputInfo[outtype] = collections.OrderedDict()
        for output, path in outputPath.items():
            if outtype == "report":
                label = "%s report" % (output,)
            elif outtype == "invalid":
                label = "invalid %s input report" % (output,)
            elif outtype == "filter":
                label = "'%s' filter" % (" ".join(output),)
            elif outtype == "annotation":
                label = "'%s : %s' annotation" % (
                    " ".join(output[0][1:]),
                    " ".join(output[1]),
                )
            elif outtype == "models":
                if output[0] == output[1]:
                    label = "'%s' models" % (" ".join(output[0]),)
                else:
                    label = "'%s : %s' models" % (
                        " ".join(output[0]),
                        " ".join(output[1]),
                    )
            elif outtype == "paris":
                label = "PARIS %s report" % (output,)
            else:
                raise Exception("unexpected output type")

            if options.debug_logic == "yes":
                bio.warn(
                    "%s will be written to '%s'\n"
                    % (
                        label,
                        ("<stdout>" if options.stdout == "yes" else path),
                    )  # noqa E501
                )

            if options.stdout == "yes":
                path = "<stdout>"
            elif path in pathUsed:
                sys.exit(
                    "ERROR: cannot write %s to '%s', file is already reserved for %s\n"  # noqa E501
                    % (label, path, pathUsed[path])
                )
            elif os.path.exists(path):
                if options.overwrite == "yes":
                    bio.warn(
                        "WARNING: %s file '%s' already exists and will be overwritten\n"  # noqa E501
                        % (label, path)
                    )
                else:
                    sys.exit(
                        "ERROR: %s file '%s' already exists, must specify --overwrite or a different --prefix\n"  # noqa E501
                        % (label, path)
                    )
            pathUsed[path] = label
            file = (
                sys.stdout
                if options.stdout == "yes"
                else (open(path, "wb") if outtype != "invalid" else None)
            )
            typeOutputInfo[outtype][output] = (label, path, file)
            if outtype == "invalid":
                cb[output] = cbMake(output)
        # foreach output of type
    # foreach output type

    # attach the knowledge file, if provided
    if options.knowledge:
        dbPath = options.knowledge
        if not os.path.exists(dbPath):
            cwdDir = os.path.dirname(
                os.path.realpath(os.path.abspath(os.getcwd()))
            )  # noqa E501
            myDir = os.path.dirname(
                os.path.realpath(os.path.abspath(__file__))
            )  # noqa E501
            if not os.path.samefile(cwdDir, myDir):
                dbPath = os.path.join(myDir, options.knowledge)
                if not os.path.exists(dbPath):
                    sys.exit(
                        "ERROR: knowledge database file '%s' not found in '%s' or '%s'"  # noqa E501
                        % (options.knowledge, cwdDir, myDir)
                    )
            else:
                sys.exit(
                    "ERROR: knowledge database file '%s' not found"
                    % (options.knowledge)
                )
        bio.attachDatabaseFile(dbPath)
    # if knowledge

    # verify the replication fingerprint, if requested
    sourceVerify = collections.defaultdict(lambda: [None, None, None])
    for source, version in options.verify_source_loader or empty:
        sourceVerify[source][0] = version
    for source, option, value in options.verify_source_option or empty:
        if not sourceVerify[source][1]:
            sourceVerify[source][1] = dict()
        sourceVerify[source][1][option] = value
    for source, file, date, size, md5 in options.verify_source_file or empty:
        if not sourceVerify[source][2]:
            sourceVerify[source][2] = dict()
        sourceVerify[source][2][file] = (date, int(size), md5)
    if (
        sourceVerify
        or options.verify_biofilter_version
        or options.verify_loki_version  # noqa E501
    ):  # noqa E501
        bio.logPush("verifying replication fingerprint ...\n")
        if options.verify_biofilter_version and (
            options.verify_biofilter_version != Biofilter.getVersionString()
        ):
            sys.exit(
                "ERROR: configuration requires Biofilter version %s, but this is version %s\n"  # noqa E501
                % (
                    options.verify_biofilter_version,
                    Biofilter.getVersionString(),
                )  # noqa E501
            )
        if options.verify_loki_version and (
            options.verify_loki_version != loki_db.Database.getVersionString()
        ):
            sys.exit(
                "ERROR: configuration requires LOKI version %s, but this is version %s\n"  # noqa E501
                % (
                    options.verify_loki_version,
                    loki_db.Database.getVersionString(),
                )  # noqa E501
            )
        for source in sorted(sourceVerify):
            verify = sourceVerify[source]
            sourceID = bio._loki.getSourceID(source)
            if not sourceID:
                sys.exit(
                    "ERROR: cannot verify %s fingerprint, knowledge database contains no such source\n"  # noqa E501
                    % (source,)
                )
            version = bio._loki.getSourceIDVersion(sourceID)
            if verify[0] and verify[0] != version:
                sys.exit(
                    "ERROR: configuration requires %s loader version %s, but knowledge database reports version %s\n"  # noqa E501
                    % (source, verify[0], version)
                )
            if verify[1]:
                options = bio._loki.getSourceIDOptions(sourceID)
                for opt, val in verify[1].items():
                    if opt not in options or val != options[opt]:
                        sys.exit(
                            "ERROR: configuration requires %s loader option %s = %s, but knowledge database reports setting = %s\n"  # noqa E501
                            % (source, opt, val, options.get(opt))
                        )
            if verify[2]:
                files = bio._loki.getSourceIDFiles(sourceID)
                for file, meta in verify[2].items():
                    if file not in files:
                        sys.exit(
                            "ERROR: configuration requires a specific fingerprint for %s file '%s', but knowledge database reports no such file\n"  # noqa E501
                            % (source, file)
                        )
                    # size and hash should be sufficient comparisons, and some
                    # sources (KEGG,PharmGKB) don't provide data file
                    # timestamps anyway
                    # elif meta[0] != files[file][0]:
                    # 	sys.exit("ERROR: configuration requires %s file '%s'
                    # modification date '%s', but knowledge database reports
                    # '%s'\n" % (source,file,meta[0],files[file][0]))
                    elif meta[1] != files[file][1]:
                        sys.exit(
                            "ERROR: configuration requires %s file '%s' size %s, but knowledge database reports %s\n"  # noqa E501
                            % (source, file, meta[1], files[file][1])
                        )
                    elif meta[2] != files[file][2]:
                        sys.exit(
                            "ERROR: configuration requires %s file '%s' hash '%s', but knowledge database reports '%s'\n"  # noqa E501
                            % (source, file, meta[2], files[file][2])
                        )
        # foreach source
        bio.logPop("... OK\n")
    # if verify replication fingerprint

    # set default region_match_percent/bases
    if (options.region_match_bases is not None) and (
        options.region_match_percent is None
    ):  # noqa E501
        bio.warn(
            "WARNING: ignoring default region match percent (100) in favor of user-specified region match bases (%d)\n"  # noqa E501
            % options.region_match_bases
        )
        options.region_match_percent = None
    else:
        if options.region_match_bases is None:
            options.region_match_bases = 0
        if options.region_match_percent is None:
            options.region_match_percent = 100.0
    # if rmb/rmp

    # set the PRNG seed, if requested
    if options.random_number_generator_seed is not None:
        try:
            seed = int(options.random_number_generator_seed)
        except ValueError:
            seed = options.random_number_generator_seed or None
        bio.warn(
            "random number generator seed: %s\n"
            % (repr(seed) if (seed is not None) else "<system default>",)
        )
        random.seed(seed)
    # if rngs

    # report the genome build, if requested
    grchBuildDB, ucscBuildDB = bio.getDatabaseGenomeBuilds()
    if options.report_genome_build == "yes":
        bio.warn(
            "knowledge database genome build: GRCh%s / UCSC hg%s\n"
            % (grchBuildDB or "?", ucscBuildDB or "?")
        )
    # if genome build

    # parse input genome build version(s)
    grchBuildUser, ucscBuildUser = bio.getInputGenomeBuilds(
        options.grch_build_version, options.ucsc_build_version
    )
    if grchBuildUser or ucscBuildUser:
        bio.warn(
            "user input genome build: GRCh%s / UCSC hg%s\n"
            % (grchBuildUser or "?", ucscBuildUser or "?")
        )

    # define output helper functions
    utf8 = codecs.getencoder("utf8")

    # process reports
    for report, info in typeOutputInfo["report"].items():
        label, path, outfile = info
        bio.logPush("writing %s to '%s' ...\n" % (label, path))
        if report == "configuration":
            outfile.write(encodeLine("# Biofilter configuration file"))
            outfile.write(
                encodeLine(
                    "#   generated %s" % time.strftime("%a, %d %b %Y %H:%M:%S")
                )  # noqa E501
            )
            outfile.write(
                encodeLine(
                    "#   Biofilter version %s" % Biofilter.getVersionString()
                )  # noqa E501
            )
            outfile.write(
                encodeLine(
                    "#   LOKI version %s" % loki_db.Database.getVersionString()
                )  # noqa E501
            )
            outfile.write(encodeLine(""))
            if options.report_replication_fingerprint == "yes":
                outfile.write(
                    encodeLine(
                        '%-35s "%s"'
                        % (
                            "VERIFY_BIOFILTER_VERSION",
                            Biofilter.getVersionString(),
                        )
                    )
                )
                outfile.write(
                    encodeLine(
                        '%-35s "%s"'
                        % (
                            "VERIFY_LOKI_VERSION",
                            loki_db.Database.getVersionString(),
                        )
                    )
                )
                for source, fingerprint in bio.getSourceFingerprints().items():
                    outfile.write(
                        encodeLine(
                            '%-35s %s "%s"'
                            % ("VERIFY_SOURCE_LOADER", source, fingerprint[0])
                        )
                    )
                    for srcopt in sorted(fingerprint[1]):
                        outfile.write(
                            encodeLine(
                                "%-35s %s %s "
                                % ("VERIFY_SOURCE_OPTION", source, srcopt),
                                term="",
                            )
                        )
                        outfile.write(
                            encodeRow(fingerprint[1][srcopt], delim=" ")
                        )  # noqa E501
                    for srcfile in sorted(fingerprint[2]):
                        outfile.write(
                            encodeLine(
                                '%-35s %s "%s" '
                                % ("VERIFY_SOURCE_FILE", source, srcfile),
                                term="",
                            )
                        )
                        outfile.write(
                            encodeRow(
                                (
                                    ('"%s"' % col)
                                    for col in fingerprint[2][srcfile]  # noqa E501
                                ),  # noqa E501
                                delim=" ",
                            )
                        )
                    outfile.write(encodeLine(""))
            for opt in options:
                if opt in (
                    "configuration",
                    "verify_source_loader",
                    "verify_source_option",
                    "verify_source_file",
                ) or not hasattr(options, opt):
                    continue
                val = getattr(options, opt)
                if type(val) is bool:  # --end-of-line, --debug-*
                    continue
                opt = "%-35s" % opt.upper().replace("-", "_")
                # three possibilities: simple value, list of simple values, or
                # list of lists of simple values
                if (
                    isinstance(val, list)
                    and len(val)
                    and isinstance(val[0], list)  # noqa E501
                ):  # noqa E501
                    for subvals in val:
                        if len(subvals):
                            outfile.write(
                                encodeRow(
                                    itertools.chain([opt], subvals), delim=" "
                                )  # noqa E501
                            )
                        else:
                            outfile.write(encodeLine(opt))
                elif isinstance(val, list):
                    if len(val):
                        outfile.write(
                            encodeRow(itertools.chain([opt], val), delim=" ")
                        )  # noqa E501
                    else:
                        outfile.write(encodeLine(opt))
                elif val is not None:
                    outfile.write(encodeRow([opt, val], delim=" "))
            # foreach option
        elif report == "gene name statistics":
            outfile.write(encodeRow(["#type", "names", "unique", "ambiguous"]))
            for row in bio.generateGeneNameStats():
                outfile.write(encodeRow(row))
        elif report == "group name statistics":
            outfile.write(encodeRow(["#type", "names", "unique", "ambiguous"]))
            for row in bio.generateGroupNameStats():
                outfile.write(encodeRow(row))
        elif report == "LD profiles":
            outfile.write(
                encodeRow(["#ldprofile", "description", "metric", "value"])
            )  # noqa E501
            for row in bio.generateLDProfiles():
                outfile.write(encodeRow(row))
        else:
            raise Exception("unexpected report type")
        # which report
        if outfile != sys.stdout:
            outfile.close()
        bio.logPop("... OK\n")
    # foreach report

    # load user-defined knowledge, if any
    for path in options.user_defined_knowledge or empty:
        bio.loadUserKnowledgeFile(
            path,
            options.gene_identifier_type,
            errorCallback=cb["userknowledge"],  # noqa E501
        )
    if options.user_defined_filter != "no":
        bio.applyUserKnowledgeFilter((options.user_defined_filter == "group"))

    # apply primary filters
    for snpList in options.snp or empty:
        bio.intersectInputSNPs(
            "main",
            bio.generateRSesFromText(
                snpList, separator=":", errorCallback=cb["SNP"]
            ),  # noqa E501
            errorCallback=cb["SNP"],
        )
    for snpFileList in options.snp_file or empty:
        bio.intersectInputSNPs(
            "main",
            bio.generateRSesFromRSFiles(snpFileList, errorCallback=cb["SNP"]),
            errorCallback=cb["SNP"],
        )
    for positionList in options.position or empty:
        bio.intersectInputLoci(
            "main",
            bio.generateLiftOverLoci(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateLociFromText(
                    positionList,
                    separator=":",
                    applyOffset=True,
                    errorCallback=cb["position"],
                ),
                errorCallback=cb["position"],
            ),
            errorCallback=cb["position"],
        )
    for positionFileList in options.position_file or empty:
        bio.intersectInputLoci(
            "main",
            bio.generateLiftOverLoci(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateLociFromMapFiles(
                    positionFileList,
                    applyOffset=True,
                    errorCallback=cb["position"],  # noqa E501
                ),
                errorCallback=cb["position"],
            ),
            errorCallback=cb["position"],
        )
    for geneList in options.gene or empty:
        bio.intersectInputGenes(
            "main",
            bio.generateNamesFromText(
                geneList,
                options.gene_identifier_type,
                separator=":",
                errorCallback=cb["gene"],
            ),
            errorCallback=cb["gene"],
        )
    for geneFileList in options.gene_file or empty:
        bio.intersectInputGenes(
            "main",
            bio.generateNamesFromNameFiles(
                geneFileList,
                options.gene_identifier_type,
                errorCallback=cb["gene"],  # noqa E501
            ),
            errorCallback=cb["gene"],
        )
    for geneSearch in options.gene_search or empty:
        bio.intersectInputGeneSearch(
            "main", (2 * (encodeString(s),) for s in geneSearch)
        )
    for regionList in options.region or empty:
        bio.intersectInputRegions(
            "main",
            bio.generateLiftOverRegions(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateRegionsFromText(
                    regionList,
                    separator=":",
                    applyOffset=True,
                    errorCallback=cb["region"],
                ),
                errorCallback=cb["region"],
            ),
            errorCallback=cb["region"],
        )
    for regionFileList in options.region_file or empty:
        bio.intersectInputRegions(
            "main",
            bio.generateLiftOverRegions(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateRegionsFromFiles(
                    regionFileList,
                    applyOffset=True,
                    errorCallback=cb["region"],  # noqa E501
                ),
                errorCallback=cb["region"],
            ),
            errorCallback=cb["region"],
        )
    for groupList in options.group or empty:
        bio.intersectInputGroups(
            "main",
            bio.generateNamesFromText(
                groupList,
                options.group_identifier_type,
                separator=":",
                errorCallback=cb["group"],
            ),
            errorCallback=cb["group"],
        )
    for groupFileList in options.group_file or empty:
        bio.intersectInputGroups(
            "main",
            bio.generateNamesFromNameFiles(
                groupFileList,
                options.group_identifier_type,
                errorCallback=cb["group"],  # noqa E501
            ),
            errorCallback=cb["group"],
        )
    for groupSearch in options.group_search or empty:
        bio.intersectInputGroupSearch(
            "main", (2 * (encodeString(s),) for s in groupSearch)
        )
    for sourceList in options.source or empty:
        bio.intersectInputSources(
            "main", sourceList, errorCallback=cb["source"]
        )  # noqa E501
    for sourceFile in itertools.chain(*(options.source_file or empty)):
        bio.intersectInputSources(
            "main",
            itertools.chain(*(line for line in open(sourceFile, "r"))),
            errorCallback=cb["source"],
        )

    # apply alternate filters
    for snpList in options.alt_snp or empty:
        bio.intersectInputSNPs(
            "alt",
            bio.generateRSesFromText(
                snpList, separator=":", errorCallback=cb["alt-SNP"]
            ),
            errorCallback=cb["alt-SNP"],
        )
    for snpFileList in options.alt_snp_file or empty:
        bio.intersectInputSNPs(
            "alt",
            bio.generateRSesFromRSFiles(
                snpFileList, errorCallback=cb["alt-SNP"]
            ),  # noqa E501
            errorCallback=cb["alt-SNP"],
        )
    for positionList in options.alt_position or empty:
        bio.intersectInputLoci(
            "alt",
            bio.generateLiftOverLoci(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateLociFromText(
                    positionList,
                    separator=":",
                    applyOffset=True,
                    errorCallback=cb["alt-position"],
                ),
                errorCallback=cb["alt-position"],
            ),
            errorCallback=cb["alt-position"],
        )
    for positionFileList in options.alt_position_file or empty:
        bio.intersectInputLoci(
            "alt",
            bio.generateLiftOverLoci(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateLociFromMapFiles(
                    positionFileList,
                    applyOffset=True,
                    errorCallback=cb["alt-position"],  # noqa E501
                ),
                errorCallback=cb["alt-position"],
            ),
            errorCallback=cb["alt-position"],
        )
    for geneList in options.alt_gene or empty:
        bio.intersectInputGenes(
            "alt",
            bio.generateNamesFromText(
                geneList,
                options.gene_identifier_type,
                separator=":",
                errorCallback=cb["alt-gene"],
            ),
            errorCallback=cb["alt-gene"],
        )
    for geneFileList in options.alt_gene_file or empty:
        bio.intersectInputGenes(
            "alt",
            bio.generateNamesFromNameFiles(
                geneFileList,
                options.gene_identifier_type,
                errorCallback=cb["alt-gene"],  # noqa E501
            ),
            errorCallback=cb["alt-gene"],
        )
    for geneSearch in options.alt_gene_search or empty:
        bio.intersectInputGeneSearch(
            "alt", (2 * (encodeString(s),) for s in geneSearch)
        )
    for regionList in options.alt_region or empty:
        bio.intersectInputRegions(
            "alt",
            bio.generateLiftOverRegions(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateRegionsFromText(
                    regionList,
                    separator=":",
                    applyOffset=True,
                    errorCallback=cb["alt-region"],
                ),
                errorCallback=cb["alt-region"],
            ),
            errorCallback=cb["alt-region"],
        )
    for regionFileList in options.alt_region_file or empty:
        bio.intersectInputRegions(
            "alt",
            bio.generateLiftOverRegions(
                ucscBuildUser,
                ucscBuildDB,
                bio.generateRegionsFromFiles(
                    regionFileList,
                    applyOffset=True,
                    errorCallback=cb["alt-region"],  # noqa E501
                ),
                errorCallback=cb["alt-region"],
            ),
            errorCallback=cb["alt-region"],
        )
    for groupList in options.alt_group or empty:
        bio.intersectInputGroups(
            "alt",
            bio.generateNamesFromText(
                groupList,
                options.group_identifier_type,
                separator=":",
                errorCallback=cb["alt-group"],
            ),
            errorCallback=cb["alt-group"],
        )
    for groupFileList in options.alt_group_file or empty:
        bio.intersectInputGroups(
            "alt",
            bio.generateNamesFromNameFiles(
                groupFileList,
                options.group_identifier_type,
                errorCallback=cb["alt-group"],
            ),
            errorCallback=cb["alt-group"],
        )
    for groupSearch in options.alt_group_search or empty:
        bio.intersectInputGroupSearch(
            "alt", (2 * (encodeString(s),) for s in groupSearch)
        )
    for sourceList in options.alt_source or empty:
        bio.intersectInputSources(
            "alt", sourceList, errorCallback=cb["alt-source"]
        )  # noqa E501
    for sourceFile in itertools.chain(*(options.alt_source_file or empty)):
        bio.intersectInputSources(
            "alt",
            itertools.chain(*(line for line in open(sourceFile, "r"))),
            errorCallback=cb["alt-source"],
        )

    # report invalid input, if requested
    if options.report_invalid_input == "yes":
        for modtype, lines in cbLog.items():
            if lines:
                path = (
                    "<stdout>"
                    if options.stdout == "yes"
                    else typeOutputInfo["invalid"][modtype][1]
                )
                bio.logPush(
                    "writing invalid %s input report to '%s' ...\n"
                    % (modtype, path)  # noqa E501
                )
                outfile = (
                    sys.stdout if options.stdout == "yes" else open(path, "w")
                )  # noqa E501
                outfile.write("\n".join(lines))
                outfile.write("\n")
                if outfile != sys.stdout:
                    outfile.close()
                bio.logPop("... OK: %d invalid inputs\n" % (len(lines) / 2))
        # foreach modifier/type
    # if report invalid input

    # process filters
    for types, info in typeOutputInfo["filter"].items():
        label, path, outfile = info
        bio.logPush("writing %s to '%s' ...\n" % (label, path))
        n = -1  # don't count header
        for row in bio.generateFilterOutput(types, applyOffset=True):
            n += 1
            outfile.write(encodeRow(row))
        if outfile != sys.stdout:
            outfile.close()
        bio.logPop("... OK: %d results\n" % n)
    # foreach filter

    # process annotations
    for types, info in typeOutputInfo["annotation"].items():
        typesF, typesA = types
        label, path, outfile = info
        bio.logPush("writing %s to '%s' ...\n" % (label, path))
        n = -1  # don't count header
        for row in bio.generateAnnotationOutput(
            typesF, typesA, applyOffset=True
        ):  # noqa E501
            n += 1
            outfile.write(encodeRow(row))
        if outfile != sys.stdout:
            outfile.close()
        bio.logPop("... OK: %d results\n" % n)
    # foreach annotation

    # process models
    for types, info in typeOutputInfo["models"].items():
        typesL, typesR = types
        label, path, outfile = info
        bio.logPush("writing %s to '%s' ...\n" % (label, path))
        n = -1  # don't count header
        for row in bio.generateModelOutput(typesL, typesR, applyOffset=True):
            n += 1
            outfile.write(encodeRow(row))
        if outfile != sys.stdout:
            outfile.close()
        bio.logPop("... OK: %d results\n" % n)
    # foreach model

    # process PARIS algorithm
    if typeOutputInfo["paris"]:
        # TODO html reports?
        parisGen = bio.generatePARISResults(ucscBuildUser, ucscBuildDB)
        labelS, pathS, outfileS = typeOutputInfo["paris"]["summary"]
        outfileD = None
        if "detail" in typeOutputInfo["paris"]:
            labelD, pathD, outfileD = typeOutputInfo["paris"]["detail"]
            bio.logPush(
                "writing PARIS summary and detail to '%s' and '%s' ...\n"
                % (pathS, pathD)
            )
        else:
            bio.logPush("writing PARIS summary to '%s'  ...\n" % (pathS,))
        header = next(parisGen)
        outfileS.write(encodeRow(header[:-1]))
        if outfileD:
            outfileD.write(encodeRow(header[0:2] + header[-1]))
        n = 0
        for row in parisGen:
            n += 1
            outfileS.write(encodeRow(row[:-1]))
            if outfileD:
                outfileD.write(encodeRow(row[0:2] + ("*",) + row[4:-1]))
                for rowD in row[-1]:
                    outfileD.write(encodeRow(row[0:2] + rowD))
        if outfileS != sys.stdout:
            outfileS.close()
        if outfileD and (outfileD != sys.stdout):
            outfileD.close()
        bio.logPop("... OK: %d results\n" % n)
    # if PARIS


# __main__


if __name__ == "__main__":
    main()
