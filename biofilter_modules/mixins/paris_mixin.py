# #################################################
# PARIS MIXIN
# #################################################
import collections
import random
import itertools


class ParisMixin:
    """
    Mixin class for executing the PARIS (Probabilistic Annotation Ranking for
    Informative SNPs) analysis in a Loki database.

    IMPLEMENTED METHODS:
    - [getPARISPermutationScore]:
        Computes a permutation score for a set of features based on comparing
        the real score with scores obtained from random permutations.
    - [generatePARISResults]:
        Executes the PARIS analysis, mapping user-provided SNPs and regions to
        identify significant genomic features and calculate their statistical
        relevance.
    """

    def getPARISPermutationScore(
        self,
        featureData,
        featureBin,
        binFeatures,
        realFeatures,
        numPermutations,
        maxScore=0,
    ):
        """
        Computes a permutation score for a set of features based on comparing
        the real score with scores obtained from random permutations.

        Parameters:
        - featureData (dict): Contains feature data, where `featureData[f][1]`
        indicates the presence of a specific attribute for feature `f`.
        - featureBin (dict): Maps each feature to its respective bin.
        - binFeatures (dict): Maps each bin to a list of features it contains.
        - realFeatures (list): List of real features for which the score
        calculation will be performed.
        - numPermutations (int): Number of permutations to perform to estimate
        the permutation score.
        - maxScore (int, optional): An optional limit on the score, which
        stops processing if the permutation score reaches this value.

        Functionality:
        - Computes `realScore`, which is the count of real features
        (`realFeatures`) that have valid data in their respective bins
        (`featureBin`) and a value at index 1 in `featureData`.
        - If `realScore` is less than 1, returns the number of permutations
        directly, as no relevant features are present.
        - Defines `binDraws`, a counter that stores the number of features to
        be drawn for each bin based on the bins in `realFeatures`.
        - Performs `numPermutations` random samplings, calculating a
        `permScore` for each:
            - For each bin `b`, it performs `draws` random samples using
            random.sample, count features with valid data at featureData[f][1]
            - If `permScore` in a permutation is greater than or equal to
            `realScore`, increments `totalScore`.
            - If `totalScore` reaches `maxScore`, halts permutations early.
        - Returns `totalScore`, representing the number of permutations where
        the permutation score was greater than or equal to the real score.

        Returns:
        - `totalScore` (int): Indicates the number of times the permuted score
        was greater than or equal to the real score.

        This method is useful for evaluating the significance of a real score
        for a set of features, comparing it against a permutation-based
        distribution.
        """
        realScore = sum(
            1
            for f in realFeatures
            if (featureBin.get(f) and featureData[f][1])  # noqa E501
        )
        if realScore < 1:
            return numPermutations

        # TODO: refinement?

        _sample = random.sample
        binDraws = collections.Counter(
            featureBin[f] for f in realFeatures if featureBin.get(f)
        )
        totalScore = 0
        for p in range(numPermutations):
            permScore = 0
            for b, draws in binDraws.items():
                permScore += sum(
                    1
                    for f in _sample(binFeatures[b], draws)
                    if featureData[f][1]  # noqa E501
                )
            if permScore >= realScore:
                totalScore += 1
                if maxScore and (totalScore >= maxScore):
                    break
        return totalScore

    def generatePARISResults(self, ucscBuildUser, ucscBuildDB):
        """
        Executes the PARIS (Probabilistic Annotation Ranking for Informative
        SNPs) analysis, mapping user-provided SNPs and regions to identify
        significant genomic features and calculate their statistical relevance.

        Parameters:
        - ucscBuildUser: UCSC genome version for user-provided data.
        - ucscBuildDB: UCSC genome version for the genomic knowledge database.

        Main Steps:
        1. **Data Preparation**:
        - Extracts and organizes genomic regions (features) from the database,
        extending region boundaries and dividing them into zones for efficient
        querying.
        - Configures parameters such as position margin and p-value threshold
        for SNP significance.

        2. **SNP and Region Mapping**:
        - Maps SNPs and positions to determine overlaps within feature regions.
        - Analyzes matches to identify SNPs/positions as valid, isolated, or
        insignificant.

        3. **Region Binning**:
        - Organizes features into bins of similar size to improve permutation
        calculation efficiency.
        - Creates special bins for features of size 0 and 1 and adjusts
        remaining bins for balanced distribution.

        4. **Mapping of Groups and Genes**:
        - Identifies and records genes and gene groups associated with the
        features.
        - Constructs datasets linking specific features to each gene and group
        for downstream analysis.

        5. **Permutation Calculations**:
        - Computes a permutation score for each gene and group, estimating
        statistical significance of features.
        - Uses real matches and feature distribution to calculate a p-value.

        6. **Result Generation**:
        - Produces a table with groups, genes, and feature data, including
        counts of simple and complex regions, and p-values.
        - Returns:
            - Group ID and name.
            - Number of genes and features linked to the group.
            - Counts of simple and complex regions.
            - Calculated p-values from permutations.

        Returns:
        - A generator yielding organized results with significant features,
        regions, and statistical values for SNPs and genes relative to genomic
        database knowledge.

        Notes:
        - This method is ideal for genomic studies seeking to understand SNP
        and gene associations with relevant genomic regions, considering the
        statistical context of identified features.
        """
        self.logPush("running PARIS ...\n")
        cursor = self._loki._biofilter.db.cursor()

        if not self._inputFilters["main"]["region"]:
            raise Exception("PARIS requires input feature regions")

        empty = list()
        threshold = self._options.paris_p_value
        rpMargin = self._options.region_position_margin
        optEnforceChm = self._options.paris_enforce_input_chromosome == "yes"
        optZeroPvals = self._options.paris_zero_p_values
        zoneSize = 100000  # in this context it doesn't have to match what the db uses  # noqa E501
        self.prepareTableForUpdate("main", "region")

        self.logPush("scanning feature regions ...\n")
        featureData = dict()  # featureData[rowid] = (size,sig)
        featureBounds = (
            dict()
        )  # featureBounds[rowid] = (rowid,chr,posMin,posMax)  # noqa E501
        chrZoneFeatures = collections.defaultdict(
            lambda: collections.defaultdict(set)
        )  # noqa E501
        sql = "SELECT rowid,chr,posMin,posMax FROM `main`.`region`"
        for fid, chm, posMin, posMax in cursor.execute(sql):
            posMin -= rpMargin
            posMax += rpMargin
            featureData[fid] = [0, 0]
            featureBounds[fid] = (fid, chm, posMin, posMax)
            for z in range(int(posMin / zoneSize), int(posMax / zoneSize) + 1):
                chrZoneFeatures[chm][z].add(fid)
        self.logPop("... OK: %d regions\n" % (len(featureData),))

        def analyzeLoci(generator):
            numMatch = numSingle = numIgnore = 0
            for chm, pos, extra in generator:
                extra = extra.split()

                if optEnforceChm:
                    try:
                        ichm = self._loki.chr_num[
                            extra[0].strip()
                        ]  # TODO optional ichm column position
                        if ichm and (ichm != chm):
                            continue
                    except:  # noqa E722
                        continue
                # if enforce input chromosome

                try:
                    # TODO optional pval column position
                    pval = float(extra[1].strip())
                    if pval <= 0.0:
                        if optZeroPvals == "significant":
                            sig = True
                        elif optZeroPvals == "insignificant":
                            sig = False
                        else:
                            numIgnore += 1
                            continue
                    else:
                        sig = pval <= threshold  # TODO <= or < ?
                except:  # noqa E722
                    sig = False

                matched = False
                for f in chrZoneFeatures[chm][pos / zoneSize]:
                    fid, fchm, fposMin, fposMax = featureBounds[f]
                    if (chm == fchm) and (pos >= fposMin) and (pos <= fposMax):
                        matched = True
                        featureData[fid][0] += 1
                        if sig:
                            featureData[fid][1] += 1
                if matched:
                    numMatch += 1
                else:
                    numSingle += 1
                    for row in cursor.execute(
                        "INSERT INTO `main`.`region` (label,chr,posMin,posMax) VALUES ('chr'|?1|':'|?2, ?1, ?2, ?2); SELECT LAST_INSERT_ROWID()",  # noqa E501
                        (chm, pos),
                    ):
                        fid = row[0]
                    posMin = pos - rpMargin
                    posMax = pos + rpMargin
                    featureData[fid] = [1, 1] if sig else [1, 0]
                    featureBounds[fid] = (fid, chm, posMin, posMax)
                    for z in range(
                        int(posMin / zoneSize), int(posMax / zoneSize) + 1
                    ):  # noqa E501
                        chrZoneFeatures[chm][z].add(fid)
            # foreach position
            return (numMatch, numSingle, numIgnore)

        # analyzeLoci()

        if self._inputFilters["main"]["snp"]:
            self.logPush("mapping SNP results to feature regions ...\n")
            querySelect = ["position_chr", "position_pos", "snp_extra"]
            queryFilter = {"main": {"snp": 1}}
            query = self.buildQuery(
                "filter",
                "main",
                select=querySelect,
                fromFilter=queryFilter,
                joinFilter=queryFilter,
            )
            numMatch, numSingle, numIgnore = analyzeLoci(
                self.generateQueryResults(query)
            )
            self.logPop(
                "... OK: %d in feature regions, %d singletons (%d ignored)\n"
                % (numMatch, numSingle, numIgnore)
            )
        # if SNPs

        if self._inputFilters["main"]["locus"]:
            self.logPush("mapping position results to feature regions ...\n")
            querySelect = ["position_chr", "position_pos", "position_extra"]
            queryFilter = {"main": {"locus": 1}}
            query = self.buildQuery(
                "filter",
                "main",
                select=querySelect,
                fromFilter=queryFilter,
                joinFilter=queryFilter,
            )
            numMatch, numSingle, numIgnore = analyzeLoci(
                self.generateQueryResults(query)
            )
            self.logPop(
                "... OK: %d in feature regions, %d singletons (%d ignored)\n"
                % (numMatch, numSingle, numIgnore)
            )
        # if loci

        for snpFileList in self._options.paris_snp_file or empty:
            self.logPush("reading SNP results ...\n")
            tallyRS = dict()
            tallyPos = dict()
            numMatch, numSingle, numIgnore = analyzeLoci(
                (
                    (chm, pos, posextra)
                    for rs, posextra, chm, pos in self._loki.generateSNPLociByRSes(  # noqa E501
                        (
                            (rsnew, rsextra)
                            for rsold, rsextra, rsnew in self._loki.generateCurrentRSesByRSes(  # noqa E501
                                self.generateRSesFromRSFiles(snpFileList),
                                tally=tallyRS,  # noqa E501
                            )
                        ),
                        minMatch=1,
                        maxMatch=(
                            None
                            if (self._options.allow_ambiguous_snps == "yes")
                            else 1  # noqa E501
                        ),
                        tally=tallyPos,
                    )
                )
            )
            self.logPop(
                "... OK: %d in feature regions, %d singletons (%d ignored, %d merged, %d unrecognized, %d ambiguous)\n"  # noqa E501
                % (
                    numMatch,
                    numSingle,
                    numIgnore,
                    tallyRS["merge"],
                    tallyPos["zero"],
                    tallyPos["many"],
                )
            )
        # foreach paris_snp_file

        for positionFileList in self._options.paris_position_file or empty:
            self.logPush("reading position results ...\n")
            numMatch, numSingle, numIgnore = analyzeLoci(
                (
                    (chm, pos, extra)
                    for label, chm, pos, extra in self.generateLiftOverLoci(
                        ucscBuildUser,
                        ucscBuildDB,
                        self.generateLociFromMapFiles(
                            positionFileList, applyOffset=True
                        ),
                    )
                )
            )
            self.logPop(
                "... OK: %d in feature regions, %d singletons (%d ignored)\n"
                % (numMatch, numSingle, numIgnore)
            )
        # foreach paris_position_file

        featureBounds = chrZoneFeatures = None

        self.logPush("binning feature regions ...\n")
        # partition features by size
        sizeFeatures = collections.defaultdict(list)
        for fid, data in featureData.items():
            sizeFeatures[data[0]].append(fid)
        # randomize within each size while building a master list in descending size order  # noqa E501
        listFeatures = list()
        for size in sorted(sizeFeatures.keys(), reverse=True):
            random.shuffle(sizeFeatures[size])
            listFeatures.extend(sizeFeatures[size])
        sizeFeatures = None
        # bin all features of size 0 and 1 with eachother (no bin size limit)
        featureBin = dict()
        binFeatures = collections.defaultdict(list)
        for b in (0, 1):
            while listFeatures and (featureData[listFeatures[-1]][0] == b):
                fid = listFeatures.pop()
                assert fid not in featureBin
                featureBin[fid] = b
                binFeatures[b].append(fid)
        # distribute all remaining features into bins of equal size, close to the target size  # noqa E501
        count = max(
            1,
            int(
                0.5 + float(len(listFeatures)) / self._options.paris_bin_size
            ),  # noqa E501
        )
        size = len(listFeatures) / count
        extra = len(listFeatures) - (count * size)
        for b in range(2, 2 + count):
            for n in range(size + (1 if ((b - 2) < extra) else 0)):
                fid = listFeatures.pop()
                assert fid not in featureBin
                featureBin[fid] = b
                binFeatures[b].append(fid)
        # report bin statistics
        for b in sorted(binFeatures):
            numSig = totalSize = 0
            minSize = maxSize = None
            for data in (featureData[f] for f in binFeatures[b]):
                numSig += 1 if data[1] else 0
                minSize = (
                    min(minSize, data[0]) if (minSize is not None) else data[0]
                )  # noqa E501
                maxSize = (
                    max(maxSize, data[0]) if (maxSize is not None) else data[0]
                )  # noqa E501
                totalSize += data[0]
            self.log(
                "bin #%d: %d features (%d significant), size %d..%d (avg %g)\n"
                % (
                    b,
                    len(binFeatures[b]),
                    numSig,
                    minSize,
                    maxSize,
                    float(totalSize) / len(binFeatures[b]),
                )
            )
        self.logPop("... OK\n")

        # cull empty feature regions from the db, to speed up region matching later  # noqa E501
        self.logPush("culling empty feature regions ...\n")
        sql = "DELETE FROM `main`.`region` WHERE rowid = ?"
        cursor.executemany(sql, itertools.izip(binFeatures[0]))
        self.logPop("... OK\n")

        self.logPush("mapping pathway genes ...\n")
        queryGroupSelect = [
            "group_id",
            "group_label",
            "group_description",
            "gene_id",
            "gene_label",
            "gene_description",
        ]
        queryGroupFilter = {
            "main": {
                "group": self._inputFilters["main"]["group"],
                "source": self._inputFilters["main"]["source"],
            }
        }
        queryGroup = self.buildQuery(
            "filter",
            "main",
            select=queryGroupSelect,
            fromFilter=queryGroupFilter,
            joinFilter=queryGroupFilter,
        )
        queryGroupU = None
        if self._inputFilters["user"]["source"]:
            queryGroupU = self.buildQuery(
                "filter",
                "main",
                select=queryGroupSelect,
                fromFilter=queryGroupFilter,
                joinFilter=queryGroupFilter,
                userKnowledge=True,
            )
        groupData = dict()
        geneData = dict()
        for (
            uid,
            ulabel,
            udesc,
            gid,
            glabel,
            gdesc,
        ) in self.generateQueryResults(  # noqa E501
            queryGroup, allowDupes=True, query2=queryGroupU
        ):
            if uid not in groupData:
                groupData[uid] = [ulabel, udesc, set()]
            groupData[uid][2].add(gid)
            if gid not in geneData:
                geneData[gid] = [glabel, gdesc]
        # foreach group/gene pair
        self.logPop(
            "... OK: %d pathways, %d genes\n" % (len(groupData), len(geneData))
        )  # noqa E501

        self.logPush("mapping gene features ...\n")
        self.prepareTableForQuery("main", "region")
        queryGeneSelect = ["region_id"]
        queryGeneWhereCol = ("d_b", "biopolymer_id")
        queryGeneWhere = dict()
        # 	queryGeneWhere[('m_r','posMin')] = {'<= d_br.posMax'} #DEBUG paris 1.1.2  # noqa E501
        queryGeneFilter = {"main": {"region_zone": 1, "region": 1}}
        n = 0
        for gid, gdata in geneData.items():
            features = set()
            queryGeneWhere[queryGeneWhereCol] = {"= %d" % (gid,)}
            queryGene = self.buildQuery(
                "filter",
                "main",
                select=queryGeneSelect,
                where=queryGeneWhere,
                fromFilter=queryGeneFilter,
                joinFilter=queryGeneFilter,
            )
            for (rid,) in self.generateQueryResults(
                queryGene, allowDupes=True
            ):  # noqa E501
                features.add(rid)
            n += len(features)
            geneData[gid].append(frozenset(features))
            # foreach feature
        # foreach gene
        self.logPop("... OK: %d matched features\n" % (n,))

        self.logPush("mapping pathway features ...\n")
        n = 0
        for uid, udata in groupData.items():
            features = set()  # TODO: allow duplicate features (build as list)
            for gid in udata[2]:
                features.update(geneData[gid][2])
            n += len(features)
            groupData[uid].append(frozenset(features))
        self.logPop("... OK: %d matched features\n" % (n,))

        # return the output generator
        self.logPop("... OK\n")

        genePvalCache = dict()

        def renderPermuPVal(realFeatures, geneID=None):
            ret = genePvalCache.get(geneID)
            if ret is not None:
                return ret
            maxScore = None
            if self._options.paris_max_p_value is not None:
                maxScore = int(
                    self._options.paris_max_p_value
                    * self._options.paris_permutation_count
                    + 0.5
                )
            realScore = self.getPARISPermutationScore(
                featureData,
                featureBin,
                binFeatures,
                realFeatures,
                self._options.paris_permutation_count,
                maxScore,
            )
            if realScore < 1:
                ret = "< %g" % (1.0 / self._options.paris_permutation_count,)
            else:
                ret = "%g" % (
                    float(realScore) / self._options.paris_permutation_count,
                )  # noqa E501
                if maxScore and (realScore >= maxScore):
                    ret = ">= " + ret
            if geneID:
                genePvalCache[geneID] = ret
            return ret

        # renderPermuPVal()

        yield (
            "id",
            "group",
            "description",
            "genes",
            "features",
            "simple",
            "(sig)",
            "complex",
            "(sig)",
            "pval",
            (
                "gene",
                "features",
                "simple",
                "(sig)",
                "complex",
                "(sig)",
                "pval",
            ),  # noqa E501
        )
        for uid, udata in groupData.items():
            yield (
                uid,
                udata[0],
                udata[1],
                len(udata[2]),
                len(udata[3]),
                sum(1 for f in udata[3] if (featureData[f][0] == 1)),
                sum(
                    1
                    for f in udata[3]
                    if (featureData[f][1] and (featureData[f][0] == 1))
                ),
                sum(1 for f in udata[3] if (featureData[f][0] > 1)),
                sum(
                    1
                    for f in udata[3]
                    if (featureData[f][1] and (featureData[f][0] > 1))
                ),
                renderPermuPVal(udata[3]),
                (
                    (
                        geneData[gid][0],
                        len(geneData[gid][2]),
                        sum(
                            1
                            for f in geneData[gid][2]
                            if (featureData[f][0] == 1)  # noqa E501
                        ),  # noqa E501
                        sum(
                            1
                            for f in geneData[gid][2]
                            if (featureData[f][1] and (featureData[f][0] == 1))
                        ),
                        sum(
                            1
                            for f in geneData[gid][2]
                            if (featureData[f][0] > 1)  # noqa E501
                        ),  # noqa E501
                        sum(
                            1
                            for f in geneData[gid][2]
                            if (featureData[f][1] and (featureData[f][0] > 1))
                        ),
                        renderPermuPVal(geneData[gid][2], gid),
                    )
                    for gid in udata[2]
                ),
            )
