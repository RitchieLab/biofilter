# #################################################
# UPDATER OPERATIONS MIXIN
# Run operations on the database after an update.
# #################################################
import logging


class UpdaterOperationsMixin:
    def cleanupSNPMerges(self):
        self.log("verifying SNP merge records ...", level=logging.INFO, indent=0)
        self.prepareTableForQuery("snp_merge")
        dbc = self._db.cursor()

        # for each set of ROWIDs which constitute a duplicated snp merge,
        # cull all but one
        cull = set()
        sql = (
            "SELECT GROUP_CONCAT(_ROWID_) "
            "FROM `db`.`snp_merge` GROUP BY rsMerged HAVING COUNT() > 1"
        )
        for row in dbc.execute(sql):
            cull.update((int(i),) for i in row[0].split(",")[1:])
        if cull:
            self.flagTableUpdate("snp_merge")
            dbc.executemany(
                "DELETE FROM `db`.`snp_merge` WHERE _ROWID_ = ?", cull  # noqa E501
            )
        self.log(
            " OK: %d duplicate merges\n" % (len(cull),), level=logging.INFO, indent=0
        )

    def updateMergedSNPLoci(self):
        self.log("checking for merged SNP loci ...", level=logging.INFO, indent=0)
        self.prepareTableForQuery("snp_locus")
        self.prepareTableForQuery("snp_merge")
        dbc = self._db.cursor()
        sql = (
            "INSERT INTO `db`.`snp_locus` (rs, chr, pos, validated, source_id) "  # noqa E501
            "SELECT sm.rsCurrent, sl.chr, sl.pos, sl.validated, sl.source_id "
            "FROM `db`.`snp_locus` AS sl "
            "JOIN `db`.`snp_merge` AS sm "
            "ON sm.rsMerged = sl.rs "
        )
        dbc.execute(sql)
        numCopied = self._db.changes()
        if numCopied:
            self.flagTableUpdate("snp_locus")
        self.log(" OK: %d loci copied\n" % (numCopied,), level=logging.INFO, indent=0)

    def cleanupSNPLoci(self):
        self.log("verifying SNP loci ...", level=logging.INFO, indent=0)
        self.prepareTableForQuery("snp_locus")
        dbc = self._db.cursor()
        # for each set of ROWIDs which constitute a duplicated snp-locus,
        # cull all but one but, make sure that if any of the originals were
        # validated, the remaining one is also
        valid = set()
        cull = set()
        sql = (
            "SELECT GROUP_CONCAT(_ROWID_), MAX(validated) "
            "FROM `db`.`snp_locus` GROUP BY rs, chr, pos HAVING COUNT() > 1"
        )
        for row in dbc.execute(sql):
            rowids = row[0].split(",")
            if row[1]:
                valid.add((int(rowids[0]),))
            cull.update((int(i),) for i in rowids[1:])
        if valid:
            dbc.executemany(
                "UPDATE `db`.`snp_locus` SET validated = 1 WHERE _ROWID_ = ?",
                valid,  # noqa E501
            )
            self.flagTableUpdate("snp_locus")
            dbc.executemany(
                "DELETE FROM `db`.`snp_locus` WHERE _ROWID_ = ?", cull  # noqa E501
            )
        self.log(
            " OK: %d duplicate loci\n" % (len(cull),), level=logging.INFO, indent=0
        )

    def updateMergedSNPEntrezRoles(self):
        self.log("checking for merged SNP roles ...", level=logging.INFO, indent=0)
        self.prepareTableForQuery("snp_entrez_role")
        self.prepareTableForQuery("snp_merge")
        dbc = self._db.cursor()
        sql = (
            "INSERT OR IGNORE INTO `db`.`snp_entrez_role` "
            "(rs, entrez_id, role_id, source_id) "
            "SELECT sm.rsCurrent, ser.entrez_id, ser.role_id, ser.source_id "
            "FROM `db`.`snp_entrez_role` AS ser "
            "JOIN `db`.`snp_merge` AS sm "
            "ON sm.rsMerged = ser.rs "
        )
        dbc.execute(sql)
        numCopied = self._db.changes()
        if numCopied:
            self.flagTableUpdate("snp_entrez_role")
        self.log(" OK: %d roles copied\n" % (numCopied,), level=logging.INFO, indent=0)

    def cleanupSNPEntrezRoles(self):
        self.log("verifying SNP roles ...", level=logging.INFO, indent=0)
        self.prepareTableForQuery("snp_entrez_role")
        dbc = self._db.cursor()
        cull = set()
        sql = (
            "SELECT GROUP_CONCAT(_ROWID_) "
            "FROM `db`.`snp_entrez_role` "
            "GROUP BY rs, entrez_id, role_id HAVING COUNT() > 1"
        )
        for row in dbc.execute(sql):
            cull.update((int(i),) for i in row[0].split(",")[1:])
        if cull:
            self.flagTableUpdate("snp_entrez_role")
            dbc.executemany(
                "DELETE FROM `db`.`snp_entrez_role` WHERE _ROWID_ = ?", cull
            )
        self.log(
            " OK: %d duplicate roles\n" % (len(cull),), level=logging.INFO, indent=0
        )

    def updateMergedGWASAnnotations(self):
        self.log(
            "checking for merged GWAS annotated SNPs ...", level=logging.INFO, indent=0
        )
        self.prepareTableForQuery("gwas")
        self.prepareTableForQuery("snp_merge")
        dbc = self._db.cursor()
        sql = (
            "INSERT INTO `db`.`gwas` "
            "(rs, chr, pos, trait, snps, orbeta, allele95ci, riskAfreq, pubmed_id, source_id) "  # noqa E501
            "SELECT sm.rsCurrent, w.chr, w.pos, w.trait, w.snps, w.orbeta, "
            "w.allele95ci, w.riskAfreq, w.pubmed_id, w.source_id "
            "FROM `db`.`gwas` AS w "
            "JOIN `db`.`snp_merge` AS sm "
            "  ON sm.rsMerged = w.rs "
        )
        dbc.execute(sql)
        numCopied = self._db.changes()
        if numCopied:
            self.flagTableUpdate("gwas")
        self.log(
            " OK: %d annotations copied\n" % (numCopied,), level=logging.INFO, indent=0
        )

    def resolveBiopolymerNames(self):
        self.log("resolving biopolymer names ...", level=logging.INFO, indent=0)
        dbc = self._db.cursor()

        # calculate confidence scores for each possible name match
        dbc.execute(
            """
            CREATE TEMP TABLE `temp`.`_biopolymer_name_name_score` (
                new_namespace_id INTEGER NOT NULL,
                new_name        VARCHAR(256) NOT NULL,
                biopolymer_id   INTEGER NOT NULL,
                polygenic       TINYINT NOT NULL,
                implication     INTEGER NOT NULL,
                PRIMARY KEY (new_namespace_id, new_name, biopolymer_id)
            )
            """
        )
        self.prepareTableForQuery("biopolymer_name_name")
        self.prepareTableForQuery("biopolymer_name")
        self.prepareTableForQuery("biopolymer")
        self.prepareTableForQuery("namespace")
        dbc.execute(
            """
            INSERT INTO `temp`.`_biopolymer_name_name_score` (
                new_namespace_id, new_name, biopolymer_id, polygenic,
                implication
            )
            /* Calculate implication score f/each possible match f/each name */
            SELECT
                bnn.new_namespace_id,
                bnn.new_name,
                bn.biopolymer_id,
                COALESCE(n.polygenic, 0) AS polygenic,
                COUNT(1) AS implication
            FROM `db`.`biopolymer_name_name` AS bnn
            JOIN `db`.`biopolymer_name` AS bn
                USING (name)
            JOIN `db`.`biopolymer` AS b
                USING (biopolymer_id)
            LEFT JOIN `db`.`namespace` AS n
                ON n.namespace_id = bnn.new_namespace_id
            WHERE bnn.namespace_id IN (0, bn.namespace_id)
            AND bnn.type_id IN (0, b.type_id)
            GROUP BY
                bnn.new_namespace_id,
                bnn.new_name,
                bn.biopolymer_id
            """
        )

        # extrapolate new biopolymer_name records
        self.prepareTableForUpdate("biopolymer_name")
        dbc.execute("DELETE FROM `db`.`biopolymer_name` WHERE source_id = 0")
        dbc.execute(
            """
            INSERT OR IGNORE INTO `db`.`biopolymer_name` (
                biopolymer_id, namespace_id, name, source_id
            )
            /* Identify specific match with the best score for each name */
            SELECT
                biopolymer_id,
                new_namespace_id,
                new_name,
                0 AS source_id
            FROM (
                /* Identify names with only one best-score match */
                SELECT
                    new_namespace_id,
                    new_name,
                    name_implication,
                    SUM(
                        CASE WHEN implication >= name_implication
                            THEN 1 ELSE 0
                        END
                    ) AS match_implication
                FROM (
                    /* Identify best score for each name */
                    SELECT
                        new_namespace_id,
                        new_name,
                        MAX(implication) AS name_implication
                    FROM `temp`.`_biopolymer_name_name_score`
                    GROUP BY new_namespace_id, new_name
                )
                JOIN `temp`.`_biopolymer_name_name_score`
                    USING (new_namespace_id, new_name)
                GROUP BY new_namespace_id, new_name
                HAVING polygenic > 0 OR match_implication = 1
            )
            JOIN `temp`.`_biopolymer_name_name_score`
                USING (new_namespace_id, new_name)
            WHERE polygenic > 0 OR implication >= name_implication
            """
        )

        # clean up
        dbc.execute("DROP TABLE `temp`.`_biopolymer_name_name_score`")
        numTotal = numUnrec = numMatch = 0
        self.prepareTableForQuery("biopolymer_name_name")
        self.prepareTableForQuery("biopolymer_name")
        self.prepareTableForQuery("biopolymer")
        numTotal = numUnrec = numMatch = 0
        # Query to count total identifiers and the unrecognized ones
        for row in dbc.execute(
            """
            SELECT COUNT(), SUM(CASE WHEN matches < 1 THEN 1 ELSE 0 END)
            FROM (
                SELECT COUNT(DISTINCT b.biopolymer_id) AS matches
                FROM `db`.`biopolymer_name_name` AS bnn
                LEFT JOIN `db`.`biopolymer_name` AS bn
                    ON bn.name = bnn.name
                    AND bnn.namespace_id IN (0, bn.namespace_id)
                LEFT JOIN `db`.`biopolymer` AS b
                    ON b.biopolymer_id = bn.biopolymer_id
                    AND bnn.type_id IN (0, b.type_id)
                GROUP BY bnn.new_namespace_id, bnn.new_name
            )
            """
        ):
            numTotal = row[0] or 0
            numUnrec = row[1] or 0

        # Query to count recognized identifiers
        for row in dbc.execute(
            """
            SELECT COUNT()
            FROM (
                SELECT 1
                FROM `db`.`biopolymer_name`
                WHERE source_id = 0
                GROUP BY namespace_id, name
            )
            """
        ):
            numMatch = row[0] or 0

        # Calc of ambiguous identifiers
        numAmbig = numTotal - numUnrec - numMatch

        # Record of the processing summary
        self.log(
            "Resolving biopolymer names completed: %d identifiers "
            "(%d ambiguous, %d unrecognized)\n" % (numMatch, numAmbig, numUnrec),
            level=logging.INFO,
            indent=0,
        )  # noqa E501

    def resolveSNPBiopolymerRoles(self):
        self.log("resolving SNP roles ...\n", level=logging.INFO, indent=0)
        dbc = self._db.cursor()

        typeID = self._loki.getTypeID("gene")
        namespaceID = self._loki.getNamespaceID("entrez_gid")
        numUnrec = 0
        if typeID and namespaceID:
            self.prepareTableForUpdate("snp_biopolymer_role")
            self.prepareTableForQuery("snp_entrez_role")
            self.prepareTableForQuery("biopolymer_name")
            dbc.execute("DELETE FROM `db`.`snp_biopolymer_role`")
            # we have to convert entrez_id to a string because the optimizer
            # won't use the index on biopolymer_name.name if the types
            # don't match
            dbc.execute(
                """
                INSERT INTO `db`.`snp_biopolymer_role` (
                    rs, biopolymer_id, role_id, source_id
                )
                SELECT
                    ser.rs,
                    bn.biopolymer_id,
                    ser.role_id,
                    ser.source_id
                FROM `db`.`snp_entrez_role` AS ser
                JOIN `db`.`biopolymer_name` AS bn
                    ON bn.namespace_id = ?
                    AND bn.name = '' || ser.entrez_id
                JOIN `db`.`biopolymer` AS b
                    ON b.biopolymer_id = bn.biopolymer_id
                    AND b.type_id = ?
                """,
                (namespaceID, typeID),
            )
            numUnrec = sum(
                row[0]
                for row in dbc.execute(
                    """
                        SELECT COUNT()
                        FROM (
                            SELECT 1
                            FROM `db`.`snp_entrez_role` AS ser
                            LEFT JOIN `db`.`biopolymer_name` AS bn
                                ON bn.namespace_id = ?
                                AND bn.name = '' || ser.entrez_id
                            LEFT JOIN `db`.`biopolymer` AS b
                                ON b.biopolymer_id = bn.biopolymer_id
                                AND b.type_id = ?
                            GROUP BY ser._ROWID_
                            HAVING MAX(b.biopolymer_id) IS NULL
                        )
                        """,
                    (namespaceID, typeID),
                )
            )
        self.prepareTableForQuery("snp_biopolymer_role")
        cull = set()
        sql = (
            "SELECT GROUP_CONCAT(_ROWID_) "
            "FROM `db`.`snp_biopolymer_role` "
            "GROUP BY rs, biopolymer_id, role_id HAVING COUNT() > 1"
        )
        for row in dbc.execute(sql):
            cull.update((int(i),) for i in row[0].split(",")[1:])
        if cull:
            self.flagTableUpdate("snp_biopolymer_role")
            dbc.executemany(
                "DELETE FROM `db`.`snp_biopolymer_role` WHERE _ROWID_ = ?",
                cull,  # noqa E501
            )

        numTotal = numSNPs = numGenes = 0
        for row in dbc.execute(
            "SELECT COUNT(), COUNT(DISTINCT rs), "
            "COUNT(DISTINCT biopolymer_id) "
            "FROM `db`.`snp_biopolymer_role`"
        ):
            numTotal = row[0]
            numSNPs = row[1]
            numGenes = row[2]
        self.log(
            "resolving SNP roles completed: %d roles (%d SNPs, %d genes; %d unrecognized)\n"  # noqa E501
            % (numTotal, numSNPs, numGenes, numUnrec),
            level=logging.INFO,
            indent=0,
        )

    # resolveSNPBiopolymerRoles()

    def resolveGroupMembers(self):
        self.log("resolving group members ...\n", level=logging.INFO, indent=0)
        dbc = self._db.cursor()

        # calculate confidence scores for each possible name match
        dbc.execute(
            """
            CREATE TEMP TABLE `temp`.`_group_member_name_score` (
                group_id      INTEGER NOT NULL,
                member        INTEGER NOT NULL,
                biopolymer_id INTEGER NOT NULL,
                polynames     INTEGER NOT NULL,
                implication   INTEGER NOT NULL,
                quality       INTEGER NOT NULL
            )
            """
        )

        self.prepareTableForQuery("group_member_name")
        self.prepareTableForQuery("biopolymer_name")
        self.prepareTableForQuery("biopolymer")
        self.prepareTableForQuery("namespace")
        dbc.execute(
            """
            INSERT INTO `temp`.`_group_member_name_score` (
                group_id, member, biopolymer_id, polynames, implication, 
                quality
            )
            /* Calc implication and quality scores for each possible match */
            SELECT
                group_id,
                member,
                biopolymer_id,
                polynames,
                COUNT(DISTINCT gmn_rowid) AS implication,
                CASE
                    WHEN polynames > 0 THEN 1000 * COUNT(DISTINCT gmn_rowid)
                    ELSE SUM(1000 / match_count)
                END AS quality
            FROM (
                /* Count possible matches for each name of each member */
                SELECT
                    gmn._ROWID_ AS gmn_rowid,
                    gmn.group_id,
                    gmn.member,
                    gmn.namespace_id,
                    gmn.name,
                    gmn.type_id,
                    polynames,
                    COUNT(DISTINCT bn.biopolymer_id) AS match_count
                FROM (
                    /* Count matchable polyregion names for each member */
                    SELECT
                        gmn.group_id,
                        gmn.member,
                        COUNT(DISTINCT CASE
                            WHEN n.polygenic > 0 THEN gmn._ROWID_
                            ELSE NULL
                        END) AS polynames
                    FROM `db`.`group_member_name` AS gmn
                    JOIN `db`.`biopolymer_name` AS bn
                        USING (name)
                    JOIN `db`.`biopolymer` AS b
                        USING (biopolymer_id)
                    LEFT JOIN `db`.`namespace` AS n
                        ON n.namespace_id = gmn.namespace_id
                    WHERE gmn.namespace_id IN (0, bn.namespace_id)
                        AND gmn.type_id IN (0, b.type_id)
                    GROUP BY gmn.group_id, gmn.member
                )
                JOIN `db`.`group_member_name` AS gmn
                    USING (group_id, member)
                JOIN `db`.`biopolymer_name` AS bn
                    USING (name)
                JOIN `db`.`biopolymer` AS b
                    USING (biopolymer_id)
                LEFT JOIN `db`.`namespace` AS n
                    ON n.namespace_id = gmn.namespace_id
                WHERE gmn.namespace_id IN (0, bn.namespace_id)
                    AND gmn.type_id IN (0, b.type_id)
                    AND (n.polygenic > 0 OR polynames = 0)
                GROUP BY
                    gmn.group_id,
                    gmn.member,
                    gmn.namespace_id,
                    gmn.name
            ) AS gmn
            JOIN `db`.`biopolymer_name` AS bn
                USING (name)
            JOIN `db`.`biopolymer` AS b
                USING (biopolymer_id)
            WHERE gmn.namespace_id IN (0, bn.namespace_id)
                AND gmn.type_id IN (0, b.type_id)
            GROUP BY group_id, member, biopolymer_id
            """
        )

        dbc.execute(
            """
            CREATE INDEX `temp`.`_group_member_name_score__group_member_biopolymer`
            ON `_group_member_name_score` (
                group_id,
                member,
                biopolymer_id
            )
            """
        )

        # generate group_biopolymer assignments with confidence scores
        self.prepareTableForUpdate("group_biopolymer")
        dbc.execute("DELETE FROM `db`.`group_biopolymer` WHERE source_id = 0")

        dbc.execute(
            """
            /* Group-biopolymer assignments with confidence scores */
            INSERT INTO `db`.`group_biopolymer` (
                group_id, biopolymer_id, specificity, implication, quality, source_id
            )
            SELECT
                group_id,
                biopolymer_id,
                MAX(specificity) AS specificity,
                MAX(implication) AS implication,
                MAX(quality) AS quality,
                0 AS source_id
            FROM (
                /* Identify spec matches with the best score for each member */
                SELECT
                    group_id,
                    member,
                    biopolymer_id,
                    CASE
                        WHEN polynames THEN 100 / member_variance
                        ELSE 100 / match_basic
                    END AS specificity,
                    CASE
                        WHEN polynames THEN 100 * implication / member_implication
                        WHEN implication = member_implication
                            THEN 100 / match_implication
                        ELSE 0
                    END AS implication,
                    CASE
                        WHEN polynames THEN 100 * quality / member_quality
                        WHEN quality = member_quality
                            THEN 100 / match_quality
                        ELSE 0
                    END AS quality
                FROM (
                    /* Identify number of matches with the best score for each member */
                    SELECT
                        group_id,
                        member,
                        polynames,
                        COUNT(DISTINCT implication) AS member_variance,
                        member_implication,
                        member_quality,
                        COUNT() AS match_basic,
                        SUM(
                            CASE
                                WHEN implication >= member_implication THEN 1
                                ELSE 0
                            END
                        ) AS match_implication,
                        SUM(
                            CASE
                                WHEN quality >= member_quality THEN 1
                                ELSE 0
                            END
                        ) AS match_quality
                    FROM (
                        /* Identify best scores for each member */
                        SELECT
                            group_id,
                            member,
                            polynames,
                            MAX(implication) AS member_implication,
                            MAX(quality) AS member_quality
                        FROM `temp`.`_group_member_name_score`
                        GROUP BY group_id, member, polynames
                    )
                    JOIN `temp`.`_group_member_name_score`
                        USING (group_id, member, polynames)
                    GROUP BY group_id, member, polynames
                )
                JOIN `temp`.`_group_member_name_score`
                    USING (group_id, member, polynames)
                GROUP BY group_id, member, biopolymer_id
            )
            GROUP BY group_id, biopolymer_id
            """
        )

        # generate group_biopolymer placeholders for unrecognized members
        self.prepareTableForUpdate("group_biopolymer")
        self.prepareTableForQuery("group_member_name")
        self.prepareTableForQuery("biopolymer_name")
        self.prepareTableForQuery("biopolymer")
        dbc.execute(
            """
            INSERT INTO `db`.`group_biopolymer` (
                group_id, biopolymer_id, specificity, implication, quality, source_id
            )
            SELECT
                group_id,
                0 AS biopolymer_id,
                COUNT() AS specificity,
                0 AS implication,
                0 AS quality,
                0 AS source_id
            FROM (
                SELECT
                    gmn.group_id
                FROM `db`.`group_member_name` AS gmn
                LEFT JOIN `db`.`biopolymer_name` AS bn
                    ON bn.name = gmn.name
                    AND gmn.namespace_id IN (0, bn.namespace_id)
                LEFT JOIN `db`.`biopolymer` AS b
                    ON b.biopolymer_id = bn.biopolymer_id
                    AND gmn.type_id IN (0, b.type_id)
                GROUP BY
                    gmn.group_id, gmn.member
                HAVING
                    MAX(b.biopolymer_id) IS NULL
            )
            GROUP BY
                group_id
            """
        )

        # clean up
        dbc.execute("DROP TABLE `temp`.`_group_member_name_score`")
        numTotal = numSourced = numMatch = numAmbig = numUnrec = 0
        self.prepareTableForQuery("group_biopolymer")
        for row in dbc.execute(
            """
            SELECT
                COALESCE(
                    SUM(CASE WHEN biopolymer_id > 0 THEN 1 ELSE 0 END), 0
                ) AS total,
                
                COALESCE(
                    SUM(CASE
                        WHEN biopolymer_id > 0 AND source_id > 0
                        THEN 1 ELSE 0
                    END), 0
                ) AS sourced,
                
                COALESCE(
                    SUM(CASE
                        WHEN biopolymer_id > 0 AND source_id = 0
                            AND specificity >= 100
                            AND implication >= 100
                            AND quality >= 100
                        THEN 1 ELSE 0
                    END), 0
                ) AS definite,
                
                COALESCE(
                    SUM(CASE
                        WHEN biopolymer_id > 0 AND source_id = 0
                            AND (specificity < 100
                                OR implication < 100
                                OR quality < 100)
                        THEN 1 ELSE 0
                    END), 0
                ) AS conditional,
                
                COALESCE(
                    SUM(CASE
                        WHEN biopolymer_id = 0 AND source_id = 0
                        THEN specificity ELSE 0
                    END), 0
                ) AS unmatched
            FROM `db`.`group_biopolymer`
            """
        ):
            numTotal = row[0]
            numSourced = row[1]
            numMatch = row[2]
            numAmbig = row[3]
            numUnrec = row[4]
        self.log(
            "Resolving group members completed: %d associations "
            "(%d explicit, %d definite, %d conditional, %d unrecognized)\n"
            % (numTotal, numSourced, numMatch, numAmbig, numUnrec),
            level=logging.INFO,
            indent=0,
        )

    def updateBiopolymerZones(self):
        self.log("calculating zone coverage ...", level=logging.INFO, indent=0)
        size = self._loki.getDatabaseSetting("zone_size", int)
        if not size:
            raise Exception(
                "ERROR: could not determine database setting 'zone_size'"
            )  # noqa E501
        dbc = self._db.cursor()

        # make sure all regions are correctly oriented
        dbc.execute(
            """
            UPDATE `db`.`biopolymer_region`
            SET posMin = posMax,
                posMax = posMin
            WHERE posMin > posMax
            """
        )

        # define zone generator
        def _zones(size, regions):
            for r in regions:
                for z in range(int(r[2] / size), int(r[3] / size) + 1):
                    yield (r[0], r[1], z)

        # feed all regions through the zone generator
        self.prepareTableForUpdate("biopolymer_zone")
        self.prepareTableForQuery("biopolymer_region")
        dbc.execute("DELETE FROM `db`.`biopolymer_zone`")
        dbc.executemany(
            """
            INSERT OR IGNORE INTO `db`.`biopolymer_zone` (
                biopolymer_id, chr, zone
            ) VALUES (?, ?, ?)
            """,
            _zones(
                size,
                self._db.cursor().execute(
                    """
                    SELECT
                        biopolymer_id,
                        chr,
                        MIN(posMin),
                        MAX(posMax)
                    FROM `db`.`biopolymer_region`
                    GROUP BY biopolymer_id, chr
                    """
                ),
            ),
        )

        # clean up
        self.prepareTableForQuery("biopolymer_zone")
        for row in dbc.execute(
            """
            SELECT COUNT(), COUNT(DISTINCT biopolymer_id)
            FROM `db`.`biopolymer_zone`
            """
        ):
            numTotal = row[0]
            numGenes = row[1]
            self.log(
                "calculating zone coverage completed: %d records (%d regions)"
                % (numTotal, numGenes),
                level=logging.INFO,
                indent=0,
            )
