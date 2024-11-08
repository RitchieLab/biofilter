# #################################################
# INPUT DATA PARSERS AND LOOKUP HELPERS MIXIN
# #################################################
import sys
import codecs


class InputDataParsersMixin:
    """
    This mixin class provides methods to parse and process various types of
    genomic input data, such as SNP identifiers, loci, regions, and genome
    builds. It is designed to be used as a supplemental component within a
    larger bioinformatics system, enabling streamlined handling of input data
    across different formats and sources.

    MAIN FUNCTIONALITY:
    - Genome Builds: Methods for retrieving compatible genome build versions.
    - SNP Identification: Methods for identifying and filtering SNPs,
    handling ambiguities.
    - Loci Parsing: Functions for extracting genomic loci from text or map
    files.
    - Region Parsing: Functions to handle genomic region data across text
    and file inputs.
    - Name Extraction: Functions for retrieving gene or SNP names from inputs.
    - User Knowledge Loading: Method to load and integrate user-provided
    data like mappings or annotations.

    IMPLEMENTED METHODS:
    - [getInputGenomeBuilds]:
        Retrieves genome build versions compatible with the system.
    - [generateMergedFilteredSNPs]:
        Searches for merged and filtered SNP identifiers, with options for
        handling ambiguities.
    - [generateRSesFromText]:
        Extracts SNP identifiers from a text-based input source.
    - [generateRSesFromRSFiles]:
        Parses SNP identifiers from one or more RS files.
    - [generateLociFromText]:
        Extracts genomic loci from a text input source.
    - [generateLociFromMapFiles]:
        Parses loci data from standard map files.
    - [generateLiftOverLoci]:
        Converts loci data across genome builds using liftOver methods.
    - [generateRegionsFromText]:
        Extracts genomic regions from a text input.
    - [generateRegionsFromFiles]:
        Parses regions from region-based input files.
    - [generateLiftOverRegions]:
        Transforms genomic regions across genome builds using liftOver.
    - [generateNamesFromText]:
        Retrieves gene or SNP names from text input.
    - [generateNamesFromNameFiles]:
        Extracts gene or SNP names from dedicated name files.
    - [loadUserKnowledgeFile]:
        Loads user-provided knowledge data, such as mappings or annotations.

    These methods collectively enable the system to efficiently interpret and
    process diverse genomic data sources, enhancing flexibility and
    integration for bioinformatics workflows.
    """

    def getInputGenomeBuilds(self, grchBuild, ucscBuild):
        """
        Retrieve compatible genome build versions based on specified GRCh and
        UCSC builds.
        """
        # Implement functionality here
        if grchBuild:
            if ucscBuild:
                if ucscBuild != (
                    self._loki.getUCSChgByGRCh(grchBuild) or ucscBuild
                ):  # noqa: E501
                    sys.exit(
                        "ERROR: specified reference genome build GRCh%d is not known to correspond to UCSC hg%d"  # noqa: E501
                        % (grchBuild, ucscBuild)
                    )
            else:
                ucscBuild = self._loki.getUCSChgByGRCh(grchBuild)
        elif ucscBuild:
            grchBuild = None
            for build in self._loki.generateGRChByUCSChg(ucscBuild):
                if grchBuild:
                    grchBuild = max(grchBuild, int(build))
                else:
                    grchBuild = int(build)
        return (grchBuild, ucscBuild)

    def generateMergedFilteredSNPs(self, snps, tally=None, errorCallback=None):
        """
        Searches for merged and filtered SNP identifiers, with an option to
        eliminate ambiguity in SNPs (i.e., duplicate SNPs).

        Parameters:
        - snps: A list of tuples `[(rsInput, extra), ...]`, where `rsInput` is
        the original SNP identifier and `extra` contains additional
        information.
        - tally (dict): An optional dictionary to store counts of merged SNPs
        and SNPs with location matches.
        - errorCallback (function): An optional function to log error messages
        for SNPs that do not meet the matching criteria.

        Operation:
        - Calls `generateCurrentRSesByRSes`, which queries the `snp_merge`
        table to retrieve updated SNP identifiers, stored in `genMerge`.
        - `snp_merge` contains mappings of old SNPs to their current
        identifiers.
        - Checks the `allow_ambiguous_snps` setting:
            - If enabled ('yes'), returns SNPs from `genMerge` directly
            without further filtering.
            - If disabled, transforms `genMerge` and passes it to
            `generateSNPLociByRSes`, which queries the `snp_locus` table to
            verify unique locations for each SNP, applying `minMatch` and
            `maxMatch` as matching limits.
        - `snp_locus` contains location information for each SNP, including
        chromosome, position, and an optional validation status.
        - Formats results and returns a sequence of tuples in the format
        `(rsInput, extra, rsCurrent)`.
        - Updates `tally` with counts of merged and located SNPs (`tallyMerge`
        and `tallyLocus`) at the end, if provided.

        Returns:
        - A generator of tuples `(rsInput, extra, rsCurrent)` for each valid
        SNP according to ambiguity and location criteria.

        This method serves as an abstraction layer for
        `generateCurrentRSesByRSes` and `generateSNPLociByRSes`, applying
        additional filters according to user settings and offering flexible
        control over ambiguous and validated SNPs.
        """
        # snps=[ (rsInput,extra),... ]
        # yield:[ (rsInput,extra,rsCurrent)
        tallyMerge = dict() if (tally is not None) else None
        tallyLocus = dict() if (tally is not None) else None
        genMerge = self._loki.generateCurrentRSesByRSes(
            snps, tally=tallyMerge
        )  # (rs,extra) -> (rsold,extra,rsnew)
        if self._options.allow_ambiguous_snps == "yes":
            for row in genMerge:
                yield row
        else:
            genMergeFormat = (
                (str(rsnew), str(rsold) + "\t" + str(rsextra or ""))
                for rsold, rsextra, rsnew in genMerge
            )  # (rsold,extra,rsnew) -> (rsnew,rsold+extra)
            genLocus = self._loki.generateSNPLociByRSes(
                genMergeFormat,
                minMatch=0,
                maxMatch=1,
                validated=(
                    None
                    if (self._options.allow_unvalidated_snp_positions == "yes")
                    else True
                ),
                tally=tallyLocus,
                errorCallback=errorCallback,
            )  # (rsnew,rsold+extra) -> (rsnew,rsold+extra,chr,pos)
            genLocusFormat = (
                tuple(posextra.split("\t", 1) + [rs])
                for rs, posextra, chm, pos in genLocus
            )  # (rsnew,rsold+extra,chr,pos) -> (rsold,extra,rsnew)
            for row in genLocusFormat:
                yield row
        # if allow_ambiguous_snps
        if tallyMerge is not None:
            tally.update(tallyMerge)
        if tallyLocus is not None:
            tally.update(tallyLocus)

    def generateRSesFromText(self, lines, separator=None, errorCallback=None):
        """
        Extracts SNP identifiers (`rs`) and additional information (`extra`)
        from a list of text lines, returning them as tuples for each valid
        line. If there is a parsing error, an optional callback is used to log
        the problematic line and index.

        Parameters:
        - lines: List of strings, where each string represents a line
        containing an SNP identifier (`rs`) and optionally additional
        information (`extra`).
        - separator: Optional separator used to split each line into columns.
        If not provided, the default `split()` separator is used.
        - errorCallback: Optional function to log errors, called with the
        problematic line and an error message if an exception occurs during
        processing.

        Operation:
        - For each line:
            - Splits the line into two columns using `split(separator, 1)`.
            `cols[0]` contains the SNP identifier (`rs`), and `cols[1]` (if
            present) contains additional information (`extra`).
            - Attempts to directly convert `cols[0]` into an integer `rs`.
            - If `cols[0]` is not a number, checks if it begins with "RS"
            (e.g., `rs123`); if so, removes "RS" and converts the remainder to
            a number.
            - Sets `extra` as `cols[1]` if present; otherwise, sets it to
            `None`.
            - Returns a tuple `(rs, extra)` for each valid line.
        - If an error occurs while processing a line, calls `errorCallback`
        (if provided and line is not the first) with the problematic line and
        the error message.

        Returns:
        - A generator of tuples `(rs, extra)` for each line containing a valid
        SNP identifier.

        This method simplifies processing of text files with SNPs and allows
        customized error handling for lines that do not match the expected
        format.
        """
        ld = 0
        for line in lines:
            ld += 1
            try:
                cols = line.strip().split(separator, 1)
                if not cols:
                    continue
                try:
                    rs = int(cols[0])
                except ValueError:
                    if cols[0].upper().startswith("RS"):
                        rs = int(cols[0][2:])
                    else:
                        raise
                extra = cols[1] if (len(cols) > 1) else None
                yield (rs, extra)
            except:  # noqa: E722  # pragma: no cover
                if (ld > 1) and errorCallback:
                    errorCallback(
                        line, "%s at index %d" % (str(sys.exc_info()[1]), ld)
                    )  # noqa E501
        # foreach line

    def generateRSesFromRSFiles(
        self, paths, separator=None, errorCallback=None
    ):  # noqa E501
        """
        Reads SNP identifiers (`rs`) and additional information (`extra`) from
        multiple text files, returning them line by line while ignoring
        comment lines. In case of an error reading a file, logs a warning
        message and calls an optional error callback.

        Parameters:
        - paths: List of input file paths. If a `path` is '-' or empty, reads
        from standard input (stdin).
        - separator: Optional separator to split each line into columns when
        reading SNPs. If not provided, uses the default split() method
        separator.
        - errorCallback: Optional function called with an error message and
        file path if an error occurs when processing a file.

        Operation:
        - For each file in `paths`:
        - Opens the file for reading (or uses stdin if `path` is '-' or empty).
        - Passes the file lines to `generateRSesFromText`, skipping comment
        lines (starting with '#').
        - For each valid line returned by `generateRSesFromText`, yields a
        tuple `(rs, extra)`.
        - If an error occurs while opening or reading a file, displays a
        warning message and, if `errorCallback` is provided, calls it with the
        file path and error message.

        Returns:
        - A generator of tuples `(rs, extra)` for each valid SNP found in all
        specified files.

        This method processes a list of SNP-containing files, supports
        standard text input, and allows custom error handling for problematic
        files.
        """
        for path in paths:
            try:
                with (
                    sys.stdin if (path == "-" or not path) else open(path, "r")
                ) as file:
                    for data in self.generateRSesFromText(
                        (line for line in file if not line.startswith("#")),
                        separator,
                        errorCallback,
                    ):
                        yield data
                # with file
            except:  # noqa: E722  # pragma: no cover
                self.warn(
                    "WARNING: error reading input file '%s': %s\n"
                    % (path, str(sys.exc_info()[1]))
                )
                if errorCallback:
                    errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
        # foreach path

    def generateLociFromText(
        self, lines, separator=None, applyOffset=False, errorCallback=None
    ):
        """
        Extracts loci information (chromosome and position) from a list of
        text lines, including optional identification of labels and additional
        data. Ignores incomplete or invalid lines and allows coordinate
        adjustment as needed.

        Parameters:
        - lines: A list of strings, where each string represents a line with
        loci information.
        - separator: Optional separator to split each line into columns. If
        not provided, uses the default split() method separator.
        - applyOffset (bool): Applies an offset to the coordinate based on the
        base setting (`coordinate_base`) if set to True.
        - errorCallback: Optional function called with an error message and
        line in case of an exception during line processing.

        Operation:
        - For each line:
            - Splits the line into up to five columns using `split(separator,
            4)` and determines:
                - `chm`: The chromosome, validated and converted to an
                internal number if it begins with "CHR".
                - `label`: An optional identifier; if missing, it is generated
                based on the chromosome and position.
                - `pos`: The coordinate position; can be adjusted with
                `applyOffset` and validated against "NA" or "-".
                - `extra`: Optional additional data in the fifth column, if
                present.
        - Returns a tuple `(label, chm, pos, extra)` for each valid line.
        - If an error occurs in processing a line, calls `errorCallback` (if
        provided and line is not the first) with the problematic line and
        error message.

        Returns:
        - A generator of tuples `(label, chm, pos, extra)` for each valid
        locus found in the lines.

        This method allows processing loci files with varying column
        structures, including custom error handling and coordinate adjustment,
        making it adaptable to different genomic data formats.
        """

        # parse input/output coordinate offsets
        offset = (1 - self._options.coordinate_base) if applyOffset else 0

        ld = 0
        for line in lines:
            ld += 1
            try:
                # parse columns
                cols = line.strip().split(separator, 4)
                label = chm = pos = extra = None
                if not cols:
                    continue
                elif len(cols) < 2:
                    raise Exception("not enough columns")
                elif len(cols) == 2:
                    chm = cols[0].upper()
                    pos = cols[1].upper()
                elif len(cols) == 3:
                    chm = cols[0].upper()
                    label = cols[1]
                    pos = cols[2].upper()
                elif len(cols) >= 4:
                    chm = cols[0].upper()
                    label = cols[1]
                    pos = cols[3].upper()
                    extra = cols[4] if (len(cols) > 4) else None

                # parse, validate and convert chromosome
                if chm.startswith("CHR"):
                    chm = chm[3:]
                if chm not in self._loki.chr_num:
                    raise Exception("invalid chromosome '%s'" % chm)
                chm = self._loki.chr_num[chm]

                # parse and convert locus label
                if not label:
                    label = "chr%s:%s" % (self._loki.chr_name[chm], pos)

                # parse and convert position
                if (pos == "-") or (pos == "NA"):
                    pos = None
                else:
                    pos = int(pos) + offset
                yield (label, chm, pos, extra)
            except:  # noqa: E722  # pragma: no cover
                if (ld > 1) and errorCallback:
                    errorCallback(
                        line, "%s at index %d" % (str(sys.exc_info()[1]), ld)
                    )  # noqa E501
        # foreach line

    def generateLociFromMapFiles(
        self, paths, separator=None, applyOffset=False, errorCallback=None
    ):
        """
        Reads loci information (chromosome and position) from multiple text
        files, returning each valid line as a tuple. Ignores comment lines and
        allows coordinate adjustment if needed. On file read errors, it
        displays a warning and optionally calls a callback.

        Parameters:
        - paths: List of file paths to read. If a `path` is '-' or empty,
        reads from standard input (stdin).
        - separator: Optional separator to split each line into columns when
        reading loci. If not provided, uses the default split() method.
        - applyOffset (bool): Applies a coordinate adjustment based on
        `coordinate_base` if set to True.
        - errorCallback: Optional function called with an error message and
        file path if an exception occurs while processing a line.

        Operation:
        - For each file in `paths`:
            - Opens the file for reading (or uses stdin if `path` is '-' or
            empty).
            - Passes the file lines to `generateLociFromText`, ignoring
            comment lines (starting with '#').
            - For each valid line returned by `generateLociFromText`,
            generates a tuple `(label, chm, pos, extra)`.
            - On file open or read errors, displays a warning and, if
            `errorCallback` is provided, calls it with the file path and error
            message.

        Returns:
        - A generator of tuples `(label, chm, pos, extra)` for each valid
        locus found across all files.

        This method facilitates processing a list of files containing loci
        information, applying coordinate adjustments and custom error handling
        , making it suitable for genomic mapping data from diverse sources.
        """
        for path in paths:
            try:
                with (
                    sys.stdin if (path == "-" or not path) else open(path, "r")
                ) as file:
                    for data in self.generateLociFromText(
                        (line for line in file if not line.startswith("#")),
                        separator,
                        applyOffset,
                        errorCallback,
                    ):
                        yield data
                # with file
            except:  # noqa: E722  # pragma: no cover
                self.warn(
                    "WARNING: error reading input file '%s': %s\n"
                    % (path, str(sys.exc_info()[1]))
                )
                if errorCallback:
                    errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
        # foreach path

    def generateLiftOverLoci(
        self, ucscBuildOld, ucscBuildNew, loci, errorCallback=None
    ):
        """
        Converts a list of genomic loci between two UCSC genome versions,
        using LiftOver chains if necessary to map coordinates.

        Parameters:
        - ucscBuildOld: The old UCSC genome version (e.g., `hg18`) from which
        loci will be converted.
        - ucscBuildNew: The new UCSC genome version (e.g., `hg38`) to which
        loci will be converted.
        - loci: List of loci to be converted, where each locus is a tuple
        `(label, chrom, pos, extra)`.
            - `label`: Identifier for the locus.
            - `chrom`: Chromosome in the old version.
            - `pos`: Position of the locus in the old version.
            - `extra`: Additional information associated with the locus.
        - errorCallback (function): Optional function called with the original
        locus if an error occurs during mapping.

        Operation:
        - Checks the provided UCSC versions:
            - If `ucscBuildOld` is unspecified, issues a warning and assumes
            the loci are in the same version as the knowledge base.
            - If `ucscBuildNew` is unspecified, issues a warning and assumes
            he knowledge base version matches the input loci.
            - If `ucscBuildOld` and `ucscBuildNew` differ, checks for
            available mapping chains for conversion.
                - If mapping chains are unavailable, the program exits with an
                error message.
        - If UCSC versions differ and chains are available:
            - Sets up an error message for loci that cannot be mapped.
            - Defines a `liftoverCallback` that calls `errorCallback` with the
            original region and error message if mapping fails.
            - Calls `self._loki.generateLiftOverLoci` to convert loci using
            `ucscBuildOld`, `ucscBuildNew`, and the error callback.
        - If `ucscBuildOld` and `ucscBuildNew` are the same or no conversion
        is required, returns the original loci.

        Returns:
        - A list of converted loci (or the original loci if no conversion was
        necessary) in the format `(label, chrom, pos, extra)`.

        This method provides a validation layer for converting loci between
        UCSC genome versions, ensuring conversion occurs only when necessary
        and mapping chains are available, with support for custom error
        handling.
        """
        # loci=[ (label,chr,pos,extra), ... ]
        newloci = loci

        if not ucscBuildOld:
            self.warn(
                "WARNING: UCSC hg# build version was not specified for position input; assuming it matches the knowledge database\n"  # noqa: E501
            )
        elif not ucscBuildNew:
            self.warn(
                "WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n"  # noqa: E501
            )
        elif ucscBuildOld != ucscBuildNew:
            if not self._loki.hasLiftOverChains(ucscBuildOld, ucscBuildNew):
                sys.exit(
                    "ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg%s to hg%s\n"  # noqa: E501
                    % (
                        oldHG or "?",
                        newHG or "?",
                    )  # noqa F821  # FIXME: oldHG not defined
                )
            liftoverError = "dropped during liftOver from hg%s to hg%s" % (
                ucscBuildOld or "?",
                ucscBuildNew or "?",
            )

            def liftoverCallback(region):
                errorCallback("\t".join(str(s) for s in region), liftoverError)

            # liftoverCallback()
            newloci = self._loki.generateLiftOverLoci(
                ucscBuildOld,
                ucscBuildNew,
                loci,
                tally=None,
                errorCallback=(liftoverCallback if errorCallback else None),
            )
        # if old!=new
        return newloci

    def generateRegionsFromText(
        self, lines, separator=None, applyOffset=False, errorCallback=None
    ):
        """
        Extracts genomic region information (chromosome and position interval)
        from a list of text lines, including optional label identification and
        additional data. Ignores incomplete or invalid lines and allows
        coordinate adjustments based on configuration.

        Parameters:
        - lines: List of strings, each representing a line with genomic region
        information.
        - separator: Optional separator to split each line into columns. If
        not provided, uses the default split() method separator.
        - applyOffset (bool): Applies an offset to coordinates based on
        `coordinate_base` if set to True, adjusting `posMin` and `posMax`
        depending on the coordinate system (closed or half-open).
        - errorCallback: Optional function called with an error message and
        line in case of an exception while processing a line.

        Operation:
        - Calculates `offsetStart` and `offsetEnd` for position adjustments
        based on `coordinate_base`:
            - If `applyOffset` is True and `regions_half_open` is enabled,
            adjusts `offsetEnd` to consider half-open coordinates.
        - For each line:
            - Splits the line into up to five columns using `split(separator,
            4)` and determines:
                - `chm`: The chromosome, validated and converted to an
                internal number if it starts with "CHR".
                - `posMin` and `posMax`: Start and end coordinates of the
                region.
                - `label`: An optional identifier; if absent, is generated
                based on the chromosome and region (`chr:start-end`).
                - `extra`: Optional additional data from the fifth column, if
                present.
            - Applies `offsetStart` and `offsetEnd` to `posMin` and `posMax`,
            adjusting positions as needed.
        - Returns a tuple `(label, chm, posMin, posMax, extra)` for each valid
        line.
        - On error processing a line, calls `errorCallback` (if provided and
        not the first line) with the problematic line and error message.

        Returns:
        - A generator of tuples `(label, chm, posMin, posMax, extra)` for each
        valid region found in the lines.

        This function streamlines processing of text files with genomic region
        information, allowing coordinate adjustment based on configuration and
        offering customized error handling.
        """

        offsetStart = offsetEnd = (
            (1 - self._options.coordinate_base) if applyOffset else 0
        )
        if applyOffset and (self._options.regions_half_open == "yes"):
            offsetEnd -= 1

        ld = 0
        for line in lines:
            ld += 1
            try:
                # parse columns
                cols = line.strip().split(separator, 4)
                label = chm = posMin = posMax = extra = None
                if not cols:
                    continue
                elif len(cols) < 3:
                    raise Exception("not enough columns")
                elif len(cols) == 3:
                    chm = cols[0].upper()
                    posMin = cols[1].upper()
                    posMax = cols[2].upper()
                elif len(cols) >= 4:
                    chm = cols[0].upper()
                    label = cols[1]
                    posMin = cols[2].upper()
                    posMax = cols[3].upper()
                    extra = cols[4] if (len(cols) > 4) else None

                # parse, validate and convert chromosome
                if chm.startswith("CHR"):
                    chm = chm[3:]
                if chm not in self._loki.chr_num:
                    raise Exception("invalid chromosome '%s'" % chm)
                chm = self._loki.chr_num[chm]

                # parse and convert region label
                if not label:
                    label = "chr%s:%s-%s" % (
                        self._loki.chr_name[chm],
                        posMin,
                        posMax,
                    )  # noqa E501

                # parse and convert positions
                if (posMin == "-") or (posMin == "NA"):
                    posMin = None
                else:
                    posMin = int(posMin) + offsetStart
                if (posMax == "-") or (posMax == "NA"):
                    posMax = None
                else:
                    posMax = int(posMax) + offsetEnd

                yield (label, chm, posMin, posMax, extra)
            except:  # noqa: E722  # pragma: no cover
                if (ld > 1) and errorCallback:
                    errorCallback(
                        line, "%s at index %d" % (str(sys.exc_info()[1]), ld)
                    )  # noqa E501
        # foreach line

    def generateRegionsFromFiles(
        self, paths, separator=None, applyOffset=False, errorCallback=None
    ):
        """
        Reads genomic region information (chromosome and position range) from
        multiple text files, returning each valid line as a tuple. Ignores
        comment lines and allows coordinate adjustments based on configuration.

        Parameters:
        - paths: List of input file paths. If `path` is '-' or empty, reads
        from standard input (stdin).
        - separator: Optional separator to split each line into columns when
        reading regions. Defaults to the split() method separator if not
        provided.
        - applyOffset (bool): Applies a coordinate offset based on
        `coordinate_base` if set to True. Adjusts `posMin` and `posMax` based
        on the coordinate system (closed or half-open).
        - errorCallback: Optional function called with an error message and
        file path if an exception occurs while processing a line.

        Operation:
        - For each file in `paths`:
        - Opens the file for reading (or uses stdin if `path` is '-' or empty).
        - Passes the file lines to `generateRegionsFromText`, skipping comment
        lines (starting with '#').
        - For each valid line returned by `generateRegionsFromText`, generates
        a tuple `(label, chm, posMin, posMax, extra)`.
        - If an error occurs when opening or reading the file, displays a
        warning message, and if `errorCallback` is provided, calls it with the
        file path and error message.

        Returns:
        - A generator of tuples `(label, chm, posMin, posMax, extra)` for each
        valid region found in all files.

        This method facilitates processing a list of files containing genomic
        region information, applying coordinate adjustments and custom error
        handling, making it ideal for processing genomic mapping data from
        diverse sources.
        """
        for path in paths:
            try:
                with (
                    sys.stdin if (path == "-" or not path) else open(path, "r")
                ) as file:
                    for data in self.generateRegionsFromText(
                        (line for line in file if not line.startswith("#")),
                        separator,
                        applyOffset,
                        errorCallback,
                    ):
                        yield data
                # with file
            except:  # noqa: E722  # pragma: no cover
                self.warn(
                    "WARNING: error reading input file '%s': %s\n"
                    % (path, str(sys.exc_info()[1]))
                )
                if errorCallback:
                    errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
        # foreach path

    def generateLiftOverRegions(
        self, ucscBuildOld, ucscBuildNew, regions, errorCallback=None
    ):
        """
        Converts a list of genomic regions between two UCSC genome versions
        using mapping chains (LiftOver chains) if needed. Checks the provided
        UCSC versions and, if they differ, maps the coordinates.

        Parameters:
        - ucscBuildOld: The old UCSC genome version (e.g., `hg18`) from which
        regions will be converted.
        - ucscBuildNew: The new UCSC genome version (e.g., `hg38`) to which
        regions will be converted.
        - regions: A list of regions to convert, where each region is a tuple
        `(label, chrom, posMin, posMax, extra)`.
            - `label`: Identifier of the region.
            - `chrom`: Chromosome in the old version.
            - `posMin` and `posMax`: Start and end coordinates in the old
            version.
            - `extra`: Additional information related to the region.
        - errorCallback (function): An optional function called with the
        original region if an error occurs during mapping.

        Operation:
        - Checks the provided UCSC versions:
            - If `ucscBuildOld` is not specified, warns that input data
            version is assumed to match the knowledge base.
            - If `ucscBuildNew` is not specified, warns that knowledge base
            version is assumed to match input data.
            - If `ucscBuildOld` and `ucscBuildNew` are different, checks for
            available mapping chains.
                - If chains are unavailable, exits with an error message.
        - If UCSC versions differ and chains are available:
            - Defines an error message for unmappable regions.
            - Sets up `liftoverCallback`, a function that calls `errorCallback`
            with the original region and error message if mapping fails.
            - Calls `self._loki.generateLiftOverRegions` to convert regions
            using `ucscBuildOld`, `ucscBuildNew`, and the error callback.
        - If `ucscBuildOld` and `ucscBuildNew` are the same or no conversion
        is needed, returns the original regions.

        Returns:
        - A list of converted regions (or original regions if no conversion
        was necessary) in the format `(label, chrom, posMin, posMax, extra)`.

        This method ensures the conversion of regions between UCSC genome
        versions only when necessary and with available mapping chains,
        allowing customized error handling.
        """

        # regions=[ (label,chr,posMin,posMax,extra), ... ]
        newregions = regions

        if not ucscBuildOld:
            self.warn(
                "WARNING: UCSC hg# build version was not specified for region input; assuming it matches the knowledge database\n"  # noqa: E501
            )
        elif not ucscBuildNew:
            self.warn(
                "WARNING: UCSC hg# build version of the knowledge database could not be determined; assuming it matches user input\n"  # noqa: E501
            )
        elif ucscBuildOld != ucscBuildNew:
            if not self._loki.hasLiftOverChains(ucscBuildOld, ucscBuildNew):
                sys.exit(
                    "ERROR: knowledge database contains no chainfiles to perform liftOver from UCSC hg%s to hg%s\n"  # noqa: E501
                    % (
                        oldHG or "?",
                        newHG or "?",
                    )  # noqa F821  # FIXME: oldHG not defined
                )
            liftoverError = "dropped during liftOver from hg%s to hg%s" % (
                ucscBuildOld or "?",
                ucscBuildNew or "?",
            )

            def liftoverCallback(region):
                errorCallback("\t".join(str(s) for s in region), liftoverError)

            # liftoverCallback()
            newregions = self._loki.generateLiftOverRegions(
                ucscBuildOld,
                ucscBuildNew,
                regions,
                tally=None,
                errorCallback=(liftoverCallback if errorCallback else None),
            )
        # if old!=new
        return newregions

    def generateNamesFromText(
        self, lines, defaultNS=None, separator=None, errorCallback=None
    ):
        """
        Extracts name and namespace information (context identifier types)
        from a list of text lines. It ignores empty or invalid lines and
        allows a default namespace if one is not specified.

        Parameters:
        - lines: A list of strings, each representing a line containing
        information for a name and optionally a namespace (ns).
        - defaultNS: Default namespace to be used if not specified in the line.
        - separator: Optional separator to split each line into columns. If
        not provided, the method uses the default `split()` separator.
        - errorCallback: Optional function called with an error message and
        the line in case of an exception while processing a line.

        Operation:
        - For each line:
            - Splits the line into up to three columns using `split(separator,
            2)` and determines:
                - `ns`: The namespace, or uses `defaultNS` if the line
                contains only a name.
                - `name`: The main name, with whitespace removed.
                - `extra`: Optional additional data contained in the third
                column, if present.
            - Returns a tuple `(ns, name, extra)` for each valid line.
        - If an error occurs while processing a line, it calls `errorCallback`
        (if provided and the line is not the first) with the problematic line
        and error message.

        Returns:
        - A generator of tuples `(ns, name, extra)` for each valid name and
        namespace found in the lines.

        This function simplifies processing text files containing names and
        namespaces, with optional error handling. It provides a default
        namespace when unspecified and allows flexibility in the input format.
        """
        # 		utf8 = codecs.getencoder('utf8')
        ld = 0
        for line in lines:
            ld += 1
            try:
                cols = line.strip().split(separator, 2)
                ns = name = extra = None
                if not cols:
                    continue
                elif len(cols) == 1:
                    ns = defaultNS
                    name = str(cols[0].strip())
                elif len(cols) >= 2:
                    ns = cols[0].strip()
                    name = str(cols[1].strip())
                    extra = cols[2] if (len(cols) > 2) else None
                yield (ns, name, extra)
            except:  # noqa: E722  # pragma: no cover
                if (ld > 1) and errorCallback:
                    errorCallback(
                        line, "%s at index %d" % (str(sys.exc_info()[1]), ld)
                    )  # noqa E501
        # foreach line in file

    def generateNamesFromNameFiles(
        self, paths, defaultNS=None, separator=None, errorCallback=None
    ):
        """
        Reads name and namespace information from multiple text files,
        returning each valid line as a tuple. Ignores comment lines and allows
        a default namespace if not specified.

        Parameters:
        - paths: List of input file paths. If `path` is '-' or empty, reads
        from standard input (stdin).
        - defaultNS: Default namespace to use if one is not specified in the
        line.
        - separator: Optional separator to split each line into columns. If
        not provided, uses the default split() method separator.
        - errorCallback: Optional function called with an error message and
        file path if an exception occurs while processing a line.

        Operation:
        - For each file in `paths`:
            - Opens the file for reading (or uses stdin if `path` is '-' or
            empty).
            - Passes the files lines to `generateNamesFromText`, ignoring
            comment lines (those starting with '#').
            - For each valid line returned by `generateNamesFromText`, yields
            a tuple `(ns, name, extra)`.
            - If an error occurs while opening or reading the file, displays a
            warning and, if `errorCallback` is provided, calls it with the
            file path and error message.

        Returns:
        - A generator of tuples `(ns, name, extra)` for each valid name and
        namespace found across all specified files.

        This method simplifies processing a list of files with name and
        namespace information, applying a default namespace as needed and
        supporting custom error handling for problematic files.
        """
        for path in paths:
            try:
                with (
                    sys.stdin if (path == "-" or not path) else open(path, "r")
                ) as file:
                    for data in self.generateNamesFromText(
                        (line for line in file if not line.startswith("#")),
                        defaultNS,
                        separator,
                        errorCallback,
                    ):
                        yield data
                # with file
            except:  # noqa: E722  # pragma: no cover
                self.warn(
                    "WARNING: error reading input file '%s': %s\n"
                    % (path, str(sys.exc_info()[1]))
                )
                if errorCallback:
                    errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
        # foreach path

    def loadUserKnowledgeFile(
        self, path, defaultNS=None, separator=None, errorCallback=None
    ):
        """
        Loads data from a file and inserts user-defined groups and sources
        into the `user.source` and `user.group` tables. This method processes
        the file line-by-line, storing information in `user` and potentially
        in `main` through additional method calls.

        Parameters:
        - path: File path for the input data to be processed. Reads from
        standard input (stdin) if set to '-' or left empty.
        - defaultNS: Default namespace to use if a namespace is not specified
        for identifiers in the file.
        - separator: Optional separator to split columns within each line of
        the file.
        - errorCallback: Optional function invoked in case of an error when
        processing a line or identifying a group.

        Operation:
        - Opens the file (`file`) and reads the first line to extract the
        `label` and `description` for the user source, calling `addUserSource`
        to add an entry in `user.source`.
        - Processes subsequent lines as instructions for groups and genes:
        - If a line starts with 'GROUP,' defines a new user group in
        `user.group` with `addUserGroup`.
        - Otherwise, treats lines as gene identifiers and stores them in
        `namesets`.
        - After each group, calls `addUserGroupBiopolymers` to insert group
        genes into `user.group_biopolymer`.
        - If an error occurs when opening or processing the file, displays a
        warning message and calls `errorCallback` if provided.

        Returns:
        - None. The method logs results and inserts data into the
        `user.source`, `user.group`, and `user.group_biopolymer` tables.

        This method allows users to define and load their data sources and
        groups, organizing and managing user-defined knowledge data in the
        `user` schema.

        """
        utf8 = codecs.getencoder("utf8")
        try:
            with (
                sys.stdin if (path == "-" or not path) else open(path, "rU")
            ) as file:  # noqa: E501
                words = utf8(file.next())[0].strip().split(separator, 1)
                label = words[0]
                description = words[1] if (len(words) > 1) else ""
                usourceID = self.addUserSource(label, description)
                ugroupID = namesets = None
                for line in file:
                    words = utf8(line)[0].strip().split(separator)
                    if not words:
                        pass
                    elif words[0] == "GROUP":
                        if ugroupID and namesets:
                            self.addUserGroupBiopolymers(
                                ugroupID, namesets, errorCallback
                            )
                        label = words[1] if (len(words) > 1) else None
                        description = " ".join(words[2:])
                        ugroupID = self.addUserGroup(
                            usourceID, label, description, errorCallback
                        )
                        namesets = list()
                    elif words[0] == "CHILDREN":
                        pass  # TODO eventual support for group hierarchies
                    elif ugroupID:
                        namesets.append(
                            list((defaultNS, w, None) for w in words)
                        )  # noqa E501
                # foreach line
                if ugroupID and namesets:
                    self.addUserGroupBiopolymers(
                        ugroupID, namesets, errorCallback
                    )  # noqa E501
            # with file
        except:  # noqa: E722  # pragma: no cover
            self.warn(
                "WARNING: error reading input file '%s': %s\n"
                % (path, str(sys.exc_info()[1]))
            )
            if errorCallback:
                errorCallback("<file> %s" % path, str(sys.exc_info()[1]))
