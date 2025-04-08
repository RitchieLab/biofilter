# #################################################
# LOKI METADATA RETRIEVAL MIXIN
# #################################################
import sys
import collections


class LokiDataRetrievalMixin:
    """
    The `LokiDataRetrievalMixin` class provides methods to retrieve specific
    data and metadata from the LOKI database, serving as a modular data access
    layer within a bioinformatics application. This mixin focuses on extracting
    key identifiers, statistics, and profiles, enabling streamlined access to
    database attributes, genome builds, and statistical data.

    MAIN FUNCTIONALITY:
    - Source Metadata: Retrieves metadata related to data sources, including
    versions, options, and files.
    - Gene and Group Statistics: Gathers statistics on gene and group names for
    analytical and reporting purposes.
    - Linkage Disequilibrium Profiles: Retrieves LD profile data for various
    genetic analyses.
    - Genome Builds: Fetches genome build information available in the database
    for compatibility checks.
    - Type and Namespace Identification: Retrieves database-specific type and
    namespace IDs for consistent data management across various inputs.

    IMPLEMENTED METHODS:
    - [getSourceFingerprints]:
        Retrieves metadata for each source in the database, including version,
        options, and associated files.
    - [generateGeneNameStats]:
        Compiles and returns statistics on gene names within the database.
    - [generateGroupNameStats]:
        Compiles and returns statistics on group names within the database.
    - [generateLDProfiles]:
        Retrieves available linkage disequilibrium (LD) profiles for analysis.
    - [getDatabaseGenomeBuilds]:
        Fetches compatible genome build versions stored in the database.
    - [getOptionTypeID]:
        Retrieves the internal ID for a specified option type, exiting if the
        type is unrecognized and `optional` is False.
    - [getOptionNamespaceID]:
        Retrieves the namespace ID associated with a given value, with an
        option to exit if the namespace is unrecognized and `optional` is F.

    The mixin organizes data retrieval for essential attributes and statistics,
    providing a structured interface for bioinformatics applications to
    interact efficiently with the LOKI database and manage diverse data attr.
    """

    # # #################################################
    # # LOKI METADATA RETRIEVAL

    def getSourceFingerprints(self):
        """
        Retrieves a structured dictionary of source fingerprints, including
        the version, options, and associated files for each source.

        Operation:
        1. Initialize Ordered Dictionary: Creates an ordered dictionary
        (`ret`) to maintain the order of source entries.
        2. Retrieve Source IDs: Gets a list of source IDs through
        `self._loki.getSourceIDs()`.
        3. Populate Fingerprints: For each source ID:
        - Retrieves the source's version with `getSourceIDVersion`.
        - Retrieves source options using `getSourceIDOptions`.
        - Collects associated files with `getSourceIDFiles`.
        - Each source's data is then added to the `ret` dictionary.

        Returns:
        - An `OrderedDict` where each key is a source, and each value is a
        tuple containing the version, options, and files associated with that
        source.

        This method provides an organized view of source data, useful for
        version control, configuration checks, and data integrity validation.
        """
        ret = collections.OrderedDict()
        sourceIDs = self._loki.getSourceIDs()
        for source in sorted(sourceIDs):
            ret[source] = (
                self._loki.getSourceIDVersion(sourceIDs[source]),
                self._loki.getSourceIDOptions(sourceIDs[source]),
                self._loki.getSourceIDFiles(sourceIDs[source]),
            )
        return ret

    def generateGeneNameStats(self):
        """
        Generates and returns statistics related to gene names by leveraging
        biopolymer data associated with genes.

        Operation:
        1. Retrieve Gene Type ID: Calls `self._loki.getTypeID("gene")` to
        obtain the unique type ID for genes.
        - If no gene type ID is found, the method terminates execution with an
        error, indicating that gene data is missing in the knowledge file.
        2. Generate Biopolymer Name Statistics: Uses the gene type ID to
        call `generateBiopolymerNameStats`, which computes and returns
        statistics on gene names.

        Returns:
        - The output from `generateBiopolymerNameStats`, which includes
        statistical data on gene names such as frequency and usage patterns.

        This method provides essential statistics on gene names, assisting in
        data analysis, name consistency checks, and frequency analysis.
        """
        typeID = self._loki.getTypeID("gene")
        if not typeID:
            sys.exit("ERROR: knowledge file contains no gene data")
        return self._loki.generateBiopolymerNameStats(typeID=typeID)

    def generateGroupNameStats(self):
        """
        Generates statistical data on group names by utilizing the
        `generateGroupNameStats` function within the loki database instance.

        Operation:
        - Directly calls `self._loki.generateGroupNameStats()`, leveraging the
        loki database internal method to gather and return statistics
        related to group names.
        - This method may involve computations on group name frequency,
        distribution, or other relevant metrics, depending on the loki
        database's specific implementation.

        Returns:
        - The output of `self._loki.generateGroupNameStats()`, which is likely
        a data structure (e.g., dictionary or DataFrame) containing various
        group name statistics.

        This function is useful for analyzing patterns, trends, or anomalies
        in group names within the database.

        """
        return self._loki.generateGroupNameStats()

    def generateLDProfiles(self):
        """
        Generates and yields linkage disequilibrium (LD) profiles, including
        relevant metadata.

        Operation:
        1. Retrieve LD Profiles: Calls `self._loki.getLDProfiles()` to
        obtain available LD profiles and their metadata.
        2. Yield Profiles: Iterates through sorted LD profiles and yields
        each one as a tuple.
        - The tuple contains the LD profile identifier followed by its
        associated metadata, skipping the first element in the profile
        metadata (`ldprofiles[ld][1:]`).

        Returns:
        - A generator that yields each LD profile and its associated metadata
        as a tuple.

        This method provides a streamlined way to access LD profiles
        sequentially, useful for applications needing to process each LD
        profile individually without loading all data into memory at once.
        """
        ldprofiles = self._loki.getLDProfiles()
        for ld in sorted(ldprofiles):
            yield (ld,) + ldprofiles[ld][1:]

    # # #################################################
    # # LOKI DATA RETRIEVAL

    def getDatabaseGenomeBuilds(self):
        """Fetches the genome build identifiers in both UCSC (e.g., `hg19`,
        `hg38`) and GRCh (e.g., `GRCh37`, `GRCh38`) formats.

        Operation:
        1. Retrieve UCSC Build: Fetches the UCSC genome build identifier
        (e.g., `hg19`) using `getDatabaseSetting("ucschg")`. Converts the
        identifier to an integer if it exists; otherwise, sets it to `None`.
        2. Initialize GRCh Build: Initializes `grchBuild` to `None`. If a
        valid UCSC build was retrieved:
        - Iterates over possible GRCh builds generated by
        `generateGRChByUCSChg`.
        - Sets the first value as the initial GRCh build.
        - Updates `grchBuild` to the highest version in the iteration.
        3. Return Tuple: Returns a tuple `(grchBuild, ucscBuild)`, where:
        - `grchBuild` is the highest GRCh build compatible with the UCSC build
        , if available.
        - `ucscBuild` is the UCSC genome build integer, if available.

        Returns:
        - A tuple `(grchBuild, ucscBuild)` representing the GRCh and UCSC
        genome build versions.

        This method allows compatibility checks across different genome
        reference builds by identifying and aligning the UCSC and GRCh build
        versions for database usage.
        """
        ucscBuild = self._loki.getDatabaseSetting("ucschg")
        ucscBuild = int(ucscBuild) if (ucscBuild is not None) else None
        grchBuild = None
        if ucscBuild:
            for build in self._loki.generateGRChByUCSChg(ucscBuild):
                if grchBuild is None:
                    grchBuild = int(build)
                    continue
                grchBuild = max(grchBuild, int(build))
        return (grchBuild, ucscBuild)

    def getOptionTypeID(self, value, optional=False):
        """
        Safely retrieves the `type_id` for a specified type, ensuring the type
        exists in the `type` table. Terminates execution if the type is not
        found, unless marked as optional.

        Parameters:
        - `value` (str): The name of the type whose `type_id` is to be
        retrieved.
        - `optional` (bool): If `True`, allows the function to return `None`
        if `type_id` is not found without terminating the program. If `False`
        (default), terminates execution if `type_id` is not found.

        Operation:
        1. Calls `getTypeID` on `self._loki` to fetch the `type_id` for the
        specified type.
        2. If `typeID` is `None` and `optional` is `False`, terminates
        execution with an error message indicating the type was not found in
        the database.
        3. Otherwise, returns the `type_id`.

        Returns:
        - `typeID`: The identifier for the specified type, or `None` if the
        type is not found and `optional` is `True`.

        Usage:
        This method provides a secure way to retrieve `type_id` values, with a
        safeguard to allow optional types that do not need to be present, thus
        preventing critical errors in non-mandatory cases.
        """
        typeID = self._loki.getTypeID(value)
        if not (typeID or optional):
            sys.exit("ERROR: database contains no %s data\n" % (value,))
        return typeID

    def getOptionNamespaceID(self, value, optional=False):
        """
        Fetches the namespace ID for a specified value, handling cases where
        the value may be optional or undefined.

        Parameters:
        - `value`: The identifier for which the namespace ID is required. If
        the value is `"-"`, it is considered a primary label, and `None` is
        returned.
        - `optional`: A boolean flag (default `False`). When `True`, allows
        for optional namespace IDs without raising an error if the ID is
        undefined.

        Operation:
        1. Checks if `value` equals `"-"`:
        - If so, returns `None` immediately, as `"-"` signifies primary labels.
        2. Retrieves the namespace ID for `value` using
        `self._loki.getNamespaceID`.
        3. If `namespaceID` is `None` and `optional` is `False`, the method
        exits with an error message indicating an unknown identifier type.

        Returns:
        - The `namespaceID` for the given `value`, or `None` if `value` is
        `"-"` or if the ID is optional and undefined.

        This method is crucial for validating and retrieving IDs, especially
        in contexts where namespace consistency is required.
        """
        if value == "-":  # primary labels
            return None
        namespaceID = self._loki.getNamespaceID(value)
        if not (namespaceID or optional):
            sys.exit("ERROR: unknown identifier type '%s'\n" % (value,))
        return namespaceID
