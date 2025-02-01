# #################################################
# SOURCE INGESTION OPERATIONS MIXIN
# #################################################
import apsw


class SourceIngestionMixin:

    ##################################################
    # metadata management
    def addLDProfile(
        self, ldprofile, description=None, metric=None, value=None
    ):  # noqa E501
        return self.addLDProfiles([(ldprofile, description, metric, value)])[
            ldprofile
        ]  # noqa E501

    # Use ABORT to avoid wasting autoincrements on existing rows,
    # and execute() to avoid stopping executemany() due to ABORT
    def addLDProfiles(self, ldprofiles):
        dbc = self._db.cursor()
        ret = {}
        insert_query = (
            "INSERT OR ABORT INTO `db`.`ldprofile` "
            "(ldprofile, description, metric, value) "
            "VALUES (LOWER(?), ?, LOWER(?), ?); "
            "SELECT LAST_INSERT_ROWID()"
        )
        select_query = "SELECT ldprofile_id FROM `db`.`ldprofile` WHERE ldprofile = LOWER(?)"  # noqa E501
        for ld in ldprofiles:
            try:
                dbc.execute(insert_query, ld)
            except apsw.ConstraintError:
                dbc.execute(select_query, ld[0:1])
            for row in dbc:
                ret[ld[0]] = row[0]
        return ret

    def addNamespace(self, namespace, polygenic=0):
        return self.addNamespaces([(namespace, polygenic)])[namespace]

    def addNamespaces(self, namespaces):
        dbc = self._db.cursor()
        ret = {}
        insert_query = (
            "INSERT OR ABORT INTO `db`.`namespace` "
            "(namespace, polygenic) VALUES (LOWER(?), ?); "
            "SELECT LAST_INSERT_ROWID()"
        )
        select_query = "SELECT namespace_id FROM `db`.`namespace` WHERE namespace = LOWER(?)"  # noqa E501
        for n in namespaces:
            try:
                dbc.execute(insert_query, n)
            except apsw.ConstraintError:
                dbc.execute(select_query, n[0:1])
            for row in dbc:
                ret[n[0]] = row[0]
        return ret

    def addRelationship(self, relationship):
        return self.addRelationships([(relationship,)])[relationship]

    def addRelationships(self, relationships):
        dbc = self._db.cursor()
        ret = {}
        insert_query = (
            "INSERT OR ABORT INTO `db`.`relationship` (relationship) "
            "VALUES (LOWER(?)); SELECT LAST_INSERT_ROWID()"
        )
        select_query = (
            "SELECT relationship_id FROM `db`.`relationship` "
            "WHERE relationship = LOWER(?)"
        )
        for r in relationships:
            try:
                dbc.execute(insert_query, r)
            except apsw.ConstraintError:
                dbc.execute(select_query, r[0:1])
            for row in dbc:
                ret[r[0]] = row[0]
        return ret

    def addRole(self, role, description=None, coding=None, exon=None):
        return self.addRoles([(role, description, coding, exon)])[role]

    def addRoles(self, roles):
        dbc = self._db.cursor()
        ret = {}
        insert_query = (
            "INSERT OR ABORT INTO `db`.`role` "
            "(role, description, coding, exon) "
            "VALUES (LOWER(?), ?, ?, ?); "
            "SELECT LAST_INSERT_ROWID()"
        )
        select_query = (
            "SELECT role_id FROM `db`.`role` WHERE role = LOWER(?)"  # noqa E501
        )
        for r in roles:
            try:
                dbc.execute(insert_query, r)
            except apsw.ConstraintError:
                dbc.execute(select_query, r[0:1])
            for row in dbc:
                ret[r[0]] = row[0]
        return ret

    def addSource(self, source):
        return self.addSources([(source,)])[source]

    def addSources(self, sources):
        dbc = self._db.cursor()
        ret = {}
        insert_query = (
            "INSERT OR ABORT INTO `db`.`source` (source) "
            "VALUES (LOWER(?)); SELECT LAST_INSERT_ROWID()"
        )
        select_query = (
            "SELECT source_id FROM `db`.`source` WHERE source = LOWER(?)"  # noqa E501
        )
        for s in sources:
            try:
                dbc.execute(insert_query, s)
            except apsw.ConstraintError:
                dbc.execute(select_query, s[0:1])
            for row in dbc:
                ret[s[0]] = row[0]
        return ret

    def addType(self, type):
        return self.addTypes([(type,)])[type]

    def addTypes(self, types):
        dbc = self._db.cursor()
        ret = {}
        insert_query = (
            "INSERT OR ABORT INTO `db`.`type` (type) "
            "VALUES (LOWER(?)); SELECT LAST_INSERT_ROWID()"
        )
        select_query = (
            "SELECT type_id FROM `db`.`type` WHERE type = LOWER(?)"  # noqa E501
        )
        for t in types:
            try:
                dbc.execute(insert_query, t)
            except apsw.ConstraintError:
                dbc.execute(select_query, t[0:1])
            for row in dbc:
                ret[t[0]] = row[0]
        return ret

    ##################################################
    # source metadata management

    def getSourceName(self):
        return self.__class__.__name__[7:]

    def getSourceID(self):
        return self._sourceID

    def setSourceBuilds(self, grch=None, ucschg=None):
        sql = (
            "UPDATE `db`.`source` SET grch = ?, ucschg = ?, "
            "current_ucschg = ?  WHERE source_id = ?"
        )
        params = (grch, ucschg, ucschg, self.getSourceID())
        self._db.cursor().execute(sql, params)

    ##################################################
    # snp data management

    def addSNPMerges(self, snpMerges):
        self.prepareTableForUpdate("snp_merge")
        sql = (
            "INSERT OR IGNORE INTO `db`.`snp_merge` "
            "(rsMerged,rsCurrent,source_id) "
            "VALUES (?,?,%d)" % (self.getSourceID(),)
        )
        with self._db:
            self._db.cursor().executemany(sql, snpMerges)

    def addSNPLoci(self, snpLoci):
        self.prepareTableForUpdate("snp_locus")
        sql = (
            "INSERT OR IGNORE INTO `db`.`snp_locus` "
            "(rs,chr,pos,validated,source_id) VALUES (?,?,?,?,%d)"
            % (self.getSourceID(),)
        )
        with self._db:
            self._db.cursor().executemany(sql, snpLoci)

    def addChromosomeSNPLoci(self, chromosome, snpLoci):
        self.prepareTableForUpdate("snp_locus")
        sql = (
            "INSERT OR IGNORE INTO `db`.`snp_locus` "
            "(rs,chr,pos,validated,source_id) VALUES (?,%d,?,?,%d)"
            % (
                chromosome,
                self.getSourceID(),
            )
        )
        with self._db:
            self._db.cursor().executemany(sql, snpLoci)

    def addSNPEntrezRoles(self, snpRoles):
        self.prepareTableForUpdate("snp_entrez_role")
        sql = (
            "INSERT OR IGNORE INTO `db`.`snp_entrez_role` "
            "(rs,entrez_id,role_id,source_id) "
            "VALUES (?,?,?,%d)" % (self.getSourceID(),)
        )
        with self._db:
            self._db.cursor().executemany(sql, snpRoles)

    ##################################################
    # biopolymer data management

    def addBiopolymers(self, biopolymers):
        self.prepareTableForUpdate("biopolymer")
        sql = (
            "INSERT INTO `db`.`biopolymer` "
            "(type_id,label,description,source_id) "
            "VALUES (?,?,?,%d); SELECT last_insert_rowid()"
            % (self.getSourceID(),)  # noqa E501
        )
        return [
            row[0]
            for row in self._db.cursor().executemany(sql, biopolymers)  # noqa E501
        ]

    def addTypedBiopolymers(self, typeID, biopolymers):
        self.prepareTableForUpdate("biopolymer")
        sql = (
            "INSERT INTO `db`.`biopolymer` "
            "(type_id,label,description,source_id) "
            "VALUES (%d,?,?,%d); SELECT last_insert_rowid()"
            % (
                typeID,
                self.getSourceID(),
            )
        )
        return [
            row[0]
            for row in self._db.cursor().executemany(sql, biopolymers)  # noqa E501
        ]

    def addBiopolymerNames(self, biopolymerNames):
        self.prepareTableForUpdate("biopolymer_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`biopolymer_name` "
            "(biopolymer_id,namespace_id,name,source_id) VALUES (?,?,?,%d)"
            % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, biopolymerNames)

    def addBiopolymerNamespacedNames(self, namespaceID, biopolymerNames):
        self.prepareTableForUpdate("biopolymer_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`biopolymer_name` "
            "(biopolymer_id,namespace_id,name,source_id) VALUES (?,%d,?,%d)"
            % (
                namespaceID,
                self.getSourceID(),
            )
        )
        self._db.cursor().executemany(sql, biopolymerNames)

    def addBiopolymerNameNames(self, biopolymerNameNames):
        self.prepareTableForUpdate("biopolymer_name_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`biopolymer_name_name` "
            "(namespace_id,name,type_id,new_namespace_id,new_name,source_id) "
            "VALUES (?,?,?,?,?,%d)" % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, biopolymerNameNames)

    def addBiopolymerTypedNameNamespacedNames(
        self, oldTypeID, newNamespaceID, biopolymerNameNames
    ):
        self.prepareTableForUpdate("biopolymer_name_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`biopolymer_name_name` "
            "(namespace_id,name,type_id,new_namespace_id,new_name,source_id) "
            "VALUES (?,?,%d,%d,?,%d)"
            % (
                oldTypeID,
                newNamespaceID,
                self.getSourceID(),
            )
        )
        self._db.cursor().executemany(sql, biopolymerNameNames)

    def addBiopolymerRegions(self, biopolymerRegions):
        self.prepareTableForUpdate("biopolymer_region")
        sql = (
            "INSERT OR IGNORE INTO `db`.`biopolymer_region` "
            "(biopolymer_id,ldprofile_id,chr,posMin,posMax,source_id) "
            "VALUES (?,?,?,?,?,%d)" % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, biopolymerRegions)

    def addBiopolymerLDProfileRegions(self, ldprofileID, biopolymerRegions):
        self.prepareTableForUpdate("biopolymer_region")
        sql = (
            "INSERT OR IGNORE INTO `db`.`biopolymer_region` "
            "(biopolymer_id,ldprofile_id,chr,posMin,posMax,source_id) "
            "VALUES (?,%d,?,?,?,%d)"
            % (
                ldprofileID,
                self.getSourceID(),
            )
        )
        self._db.cursor().executemany(sql, biopolymerRegions)

    ##################################################
    # group data management

    def addGroups(self, groups):
        self.prepareTableForUpdate("group")
        sql = (
            "INSERT INTO `db`.`group` (type_id,label,description,source_id) "
            "VALUES (?,?,?,%d); SELECT last_insert_rowid()"
            % (self.getSourceID(),)  # noqa E501
        )
        return [row[0] for row in self._db.cursor().executemany(sql, groups)]

    def addTypedGroups(self, typeID, groups):
        self.prepareTableForUpdate("group")
        sql = (
            "INSERT INTO `db`.`group` (type_id,label,description,source_id) "
            "VALUES (%d,?,?,%d); SELECT last_insert_rowid()"
            % (
                typeID,
                self.getSourceID(),
            )
        )
        return [row[0] for row in self._db.cursor().executemany(sql, groups)]

    def addGroupNames(self, groupNames):
        self.prepareTableForUpdate("group_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`group_name` "
            "(group_id,namespace_id,name,source_id) VALUES (?,?,?,%d)"
            % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, groupNames)

    def addGroupNamespacedNames(self, namespaceID, groupNames):
        self.prepareTableForUpdate("group_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`group_name` "
            "(group_id,namespace_id,name,source_id) VALUES (?,%d,?,%d)"
            % (
                namespaceID,
                self.getSourceID(),
            )
        )
        self._db.cursor().executemany(sql, groupNames)

    def addGroupRelationships(self, groupRels):
        self.prepareTableForUpdate("group_group")
        # We SHOULD be able to do (?1,?2,?3) and (?2,?1,?3) with the same
        # 3 bindings for each execution, but APSW or SQLite appears to treat
        # the compound statement separately, so we have to copy the bindings.
        base_sql = (
            "INSERT OR IGNORE INTO `db`.`group_group` "
            "(group_id, related_group_id, relationship_id, direction, "
            "contains, source_id) VALUES "
        )
        case_expr_1 = (
            "(?1, ?2, ?3, 1, (CASE WHEN ?4 IS NULL THEN NULL "
            "WHEN ?4 > 0 THEN 1 WHEN ?4 < 0 THEN -1 ELSE 0 END), %d)"
            % self.getSourceID()
        )
        case_expr_2 = (
            "(?2, ?1, ?3, -1, (CASE WHEN ?4 IS NULL THEN NULL "
            "WHEN ?4 > 0 THEN -1 WHEN ?4 < 0 THEN 1 ELSE 0 END), %d)"
            % self.getSourceID()
        )
        sql = f"{base_sql} {case_expr_1}; {base_sql} {case_expr_2}"
        self._db.cursor().executemany(sql, (2 * gr for gr in groupRels))

    def addGroupParentRelationships(self, groupRels):
        self.prepareTableForUpdate("group_group")
        base_sql = (
            "INSERT OR IGNORE INTO `db`.`group_group` "
            "(group_id, related_group_id, relationship_id, direction, "
            "contains, source_id) VALUES "
        )
        forward_relation = "(?1, ?2, ?3, 1, 1, %d)" % self.getSourceID()
        reverse_relation = "(?2, ?1, ?3, -1, -1, %d)" % self.getSourceID()
        sql = f"{base_sql} {forward_relation}; {base_sql} {reverse_relation}"
        self._db.cursor().executemany(sql, (2 * gr for gr in groupRels))

    def addGroupChildRelationships(self, groupRels):
        self.prepareTableForUpdate("group_group")
        base_sql = (
            "INSERT OR IGNORE INTO `db`.`group_group` "
            "(group_id, related_group_id, relationship_id, direction, "
            "contains, source_id) VALUES "
        )
        forward_relation = "(?1, ?2, ?3, 1, -1, %d)" % self.getSourceID()
        reverse_relation = "(?2, ?1, ?3, -1, 1, %d)" % self.getSourceID()
        sql = f"{base_sql} {forward_relation}; {base_sql} {reverse_relation}"
        self._db.cursor().executemany(sql, (2 * gr for gr in groupRels))

    def addGroupSiblingRelationships(self, groupRels):
        self.prepareTableForUpdate("group_group")
        base_sql = (
            "INSERT OR IGNORE INTO `db`.`group_group` "
            "(group_id, related_group_id, relationship_id, direction, "
            "contains, source_id) VALUES "
        )
        forward_relation = "(?1, ?2, ?3, 1, 0, %d)" % self.getSourceID()
        reverse_relation = "(?2, ?1, ?3, -1, 0, %d)" % self.getSourceID()
        sql = f"{base_sql} {forward_relation}; {base_sql} {reverse_relation}"
        self._db.cursor().executemany(sql, (2 * gr for gr in groupRels))

    def addGroupBiopolymers(self, groupBiopolymers):
        self.prepareTableForUpdate("group_biopolymer")
        sql = (
            "INSERT OR IGNORE INTO `db`.`group_biopolymer` "
            "(group_id, biopolymer_id, specificity, implication, "
            "quality, source_id) "
            "VALUES (?, ?, 100, 100, 100, %d)" % self.getSourceID()
        )
        self._db.cursor().executemany(sql, groupBiopolymers)

    def addGroupMemberNames(self, groupMemberNames):
        self.prepareTableForUpdate("group_member_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`group_member_name` "
            "(group_id,member,type_id,namespace_id,name,source_id) "
            "VALUES (?,?,?,?,?,%d)" % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, groupMemberNames)

    def addGroupMemberTypedNamespacedNames(
        self, typeID, namespaceID, groupMemberNames
    ):  # noqa E501
        self.prepareTableForUpdate("group_member_name")
        sql = (
            "INSERT OR IGNORE INTO `db`.`group_member_name` "
            "(group_id,member,type_id,namespace_id,name,source_id) "
            "VALUES (?,?,%d,%d,?,%d)"
            % (
                typeID,
                namespaceID,
                self.getSourceID(),
            )  # noqa E501
        )
        self._db.cursor().executemany(sql, groupMemberNames)

    ##################################################
    # liftover data management

    def addChains(self, old_ucschg, new_ucschg, chain_list):
        """
        Adds all of the chains described in chain_list and returns the
        IDs of the added chains. The chain_list must be an iterable
        container of objects that can be inserted into the chain table.
        """
        self.prepareTableForUpdate("chain")
        sql = (
            "INSERT INTO `db`.`chain` "
            "(score, old_ucschg, old_chr, old_start, old_end, "
            "new_ucschg, new_chr, new_start, new_end, is_fwd, source_id) "
            "VALUES (?, %d, ?, ?, ?, %d, ?, ?, ?, ?, %d); "
            "SELECT last_insert_rowid()"
            % (old_ucschg, new_ucschg, self.getSourceID())  # noqa E501
        )
        return [
            row[0] for row in self._db.cursor().executemany(sql, chain_list)
        ]  # noqa E501

    def addChainData(self, chain_data_list):
        """
        Adds all of the chain data into the chain data table
        """
        self.prepareTableForUpdate("chain_data")
        sql = (
            "INSERT INTO `db`.`chain_data` "
            "(chain_id,old_start,old_end,new_start,source_id) "
            "VALUES (?,?,?,?,%d)" % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, chain_data_list)

    ##################################################
    # gwas data management

    def addGWASAnnotations(self, gwasAnnotations):
        self.prepareTableForUpdate("gwas")
        sql = (
            "INSERT OR IGNORE INTO `db`.`gwas` "
            "(rs,chr,pos,trait,snps,orbeta,allele95ci,"
            "riskAfreq,pubmed_id,source_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,%d)" % (self.getSourceID(),)
        )
        self._db.cursor().executemany(sql, gwasAnnotations)
