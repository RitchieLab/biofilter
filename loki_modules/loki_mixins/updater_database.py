# #################################################
# UPDATER DATABASE MIXIN
# #################################################
import collections
import os
import sys
import traceback
import shutil
from threading import Thread


class UpdaterDatabaseMixin:
    def updateDatabase(
        self,
        sources=None,
        sourceOptions=None,
        cacheOnly=False,
        forceUpdate=False,  # noqa E501
    ):
        if self._updating:
            raise Exception("_updating set before updateDatabase()")
        self._loki.testDatabaseWriteable()
        if self._loki.getDatabaseSetting("finalized", int):
            raise Exception("cannot update a finalized database")

        # check for extraneous options
        self.logPush("preparing for update ...\n")
        srcSet = self.attachSourceModules(sources)
        srcOpts = sourceOptions or {}
        for srcName in srcOpts.keys():
            if srcName not in srcSet:
                self.log(
                    "WARNING: not updating from source '%s' for which options were supplied\n"  # noqa: E501
                    % srcName
                )
        logIndent = self.logPop("preparing for update completed\n")

        # update all specified sources
        iwd = os.path.abspath(os.getcwd())
        self._updating = True
        self._tablesUpdated = set()
        self._tablesDeindexed = set()
        srcErrors = set()
        cursor = self._db.cursor()
        cursor.execute("SAVEPOINT 'updateDatabase'")
        try:
            for srcName in sorted(srcSet):
                srcObj = self._sourceObjects[srcName]
                srcID = srcObj.getSourceID()

                # validate options, if any
                prevOptions = dict()
                for row in cursor.execute(
                    "SELECT option, value FROM `db`.`source_option` WHERE source_id = ?",  # noqa: E501
                    (srcID,),
                ):
                    prevOptions[str(row[0])] = str(row[1])
                options = srcOpts.get(srcName, prevOptions).copy()
                optionsList = sorted(options)
                if optionsList:
                    self.logPush(
                        "%s %s options ...\n"
                        % (
                            (
                                "validating"
                                if (srcName in srcOpts)
                                else "loading prior"
                            ),  # noqa: E501
                            srcName,
                        )
                    )
                msg = srcObj.validateOptions(options)
                if msg is not True:
                    raise Exception(msg)
                if optionsList:
                    for opt in optionsList:
                        self.log("%s = %s\n" % (opt, options[opt]))
                    self.logPop("... OK\n")

                # temp for now but should replace options everywhere below
                self._sourceOptions[srcName] = options

            downloadAndHashThreads = {}
            srcSetsToDownload = sorted(srcSet)
            for srcName in srcSetsToDownload:
                # download files into a local cache
                if not cacheOnly:
                    downloadAndHashThreads[srcName] = Thread(
                        target=self.downloadAndHash,
                        args=(
                            iwd,
                            srcName,
                            self._sourceOptions[srcName],
                        ),
                    )
                    downloadAndHashThreads[srcName].start()

            for srcName in downloadAndHashThreads.keys():
                downloadAndHashThreads[srcName].join()
                self.log(srcName + " rejoined main thread\n")

            for srcName in srcSetsToDownload:
                srcObj = self._sourceObjects[srcName]
                srcID = srcObj.getSourceID()
                options = self._sourceOptions[srcName]
                path = os.path.join(iwd, srcName)

                cursor.execute("SAVEPOINT 'updateDatabase_%s'" % (srcName,))

                try:
                    # compare current loader version, options and file
                    # metadata to the last update
                    skip = not forceUpdate
                    last = "?"
                    if skip:
                        for row in cursor.execute(
                            """
                            SELECT
                                version,
                                DATETIME(updated, 'localtime')
                            FROM `db`.`source`
                            WHERE source_id = ?
                            """,
                            (srcID,),
                        ):
                            skip = skip and (
                                row[0] == srcObj.getVersionString()
                            )  # noqa: E501
                            last = row[1]
                    if skip:
                        n = 0
                        for row in cursor.execute(
                            """
                            SELECT option, value FROM `db`.`source_option`
                            WHERE source_id = ?
                            """,
                            (srcID,),
                        ):
                            n += 1
                            skip = (
                                skip
                                and (row[0] in options)
                                and (row[1] == options[row[0]])
                            )
                        skip = skip and (n == len(options))
                    if skip:
                        n = 0
                        for row in cursor.execute(
                            """
                            SELECT filename, size, md5 FROM `db`.`source_file`
                            WHERE source_id = ?
                            """,
                            (srcID,),
                        ):
                            n += 1
                            skip = (
                                skip
                                and (row[0] in self._filehash)
                                and (row[1] == self._filehash[row[0]][1])
                                and (row[2] == self._filehash[row[0]][3])
                            )
                        skip = skip and (n == len(self._filehash))

                    # skip the update if the current loader and all source
                    # file versions match the last update
                    if skip:
                        self.log(
                            "skipping %s update, no data or software changes since %s\n"  # noqa: E501
                            % (srcName, last)
                        )
                    else:
                        # process new files (or old files with a new loader)
                        self.logPush("processing %s data ...\n" % srcName)

                        cursor.execute(
                            """
                            DELETE FROM `db`.`warning`
                            WHERE source_id = ?
                            """,
                            (srcID,),  # noqa: E501
                        )
                        srcObj.update(options, path)
                        cursor.execute(
                            """
                            UPDATE `db`.`source`
                            SET updated = DATETIME('now'),
                                version = ?
                            WHERE source_id = ?
                            """,
                            (srcObj.getVersionString(), srcID),
                        )

                        cursor.execute(
                            """
                            DELETE FROM `db`.`source_option`
                            WHERE source_id = ?
                            """,
                            (srcID,),
                        )
                        sql = (
                            "INSERT INTO `db`.`source_option` "
                            "(source_id, option, value) "
                            "VALUES (%d, ?, ?)" % srcID
                        )
                        cursor.executemany(sql, options.items())

                        cursor.execute(
                            """
                            DELETE FROM `db`.`source_file`
                            WHERE source_id = ?
                            """,
                            (srcID,),
                        )
                        sql = (
                            "INSERT INTO `db`.`source_file` "
                            "(source_id, filename, size, modified, md5) "
                            "VALUES (%d,?,?,DATETIME(?,'unixepoch'),?)" % srcID
                        )
                        cursor.executemany(sql, self._filehash.values())

                        self.logPop("processing %s data completed\n" % srcName)
                    # if skip
                except:  # noqa: E722
                    srcErrors.add(srcName)
                    excType, excVal, excTrace = sys.exc_info()
                    while self.logPop() > logIndent:
                        pass
                    self.logPush("ERROR: failed to update %s\n" % (srcName,))
                    if excTrace:
                        for line in traceback.format_list(
                            traceback.extract_tb(excTrace)[-1:]
                        ):
                            self.log(line)
                    for line in traceback.format_exception_only(
                        excType, excVal
                    ):  # noqa E501
                        self.log(line)
                    self.logPop()
                    cursor.execute(
                        "ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase_%s'"
                        % (srcName,)
                    )
                finally:
                    cursor.execute(
                        "RELEASE SAVEPOINT 'updateDatabase_%s'" % (srcName,)
                    )  # noqa: E501
                # try/except/finally

                # remove subdirectory to free up some space
                shutil.rmtree(path)
            # foreach source

            # pull the latest GRCh/UCSChg conversions
            #   http://genome.ucsc.edu/FAQ/FAQreleases.html
            #   http://genome.ucsc.edu/goldenPath/releaseLog.html
            # TODO: find a better machine-readable source for this data
            if not cacheOnly:
                self.log("updating GRCh:UCSChg genome build identities ...\n")
                import urllib.request as urllib2
                import re

                response = urllib2.urlopen(
                    "http://genome.ucsc.edu/FAQ/FAQreleases.html"
                )
                page = ""
                while True:
                    data = response.read()
                    if not data:
                        break
                    page += data.decode()
                rowHuman = False
                for tablerow in re.finditer(
                    r"<tr>.*?</tr>", page, re.IGNORECASE | re.DOTALL
                ):
                    cols = tuple(
                        match.group()[4:-5].strip().lower()
                        for match in re.finditer(
                            r"<td>.*?</td>",
                            tablerow.group(),
                            re.IGNORECASE | re.DOTALL,  # noqa: E501
                        )
                    )
                    if cols and (
                        (cols[0] == "human")
                        or (rowHuman and (cols[0] in ("", "&nbsp;")))
                    ):
                        rowHuman = True
                        grch = ucschg = None
                        try:
                            if cols[1].startswith("hg"):
                                ucschg = int(cols[1][2:])
                            if cols[3].startswith(
                                "genome reference consortium grch"
                            ):  # noqa: E501
                                grch = int(cols[3][32:])
                            if cols[3].startswith("ncbi build "):
                                grch = int(cols[3][11:])
                        except:  # noqa: E722
                            pass
                        if grch and ucschg:
                            cursor.execute(
                                """
                                INSERT OR REPLACE INTO `db`.`grch_ucschg` (
                                    grch, ucschg
                                ) VALUES (?, ?)
                                """,
                                (grch, ucschg),
                            )
                    else:
                        rowHuman = False
                # foreach tablerow
                self.log(
                    "updating GRCh:UCSChg genome build identities completed\n"  # noqa: E501
                )
            # if not cacheOnly

            # cross-map GRCh/UCSChg build versions for all sources
            ucscGRC = collections.defaultdict(int)
            for row in self._db.cursor().execute(
                "SELECT grch,ucschg FROM `db`.`grch_ucschg`"
            ):
                ucscGRC[row[1]] = max(row[0], ucscGRC[row[1]])
                cursor.execute(
                    """
                    UPDATE `db`.`source` SET grch = ?
                    WHERE grch IS NULL AND ucschg = ?
                    """,
                    (row[0], row[1]),
                )
                cursor.execute(
                    """
                    UPDATE `db`.`source` SET ucschg = ?
                    WHERE ucschg IS NULL AND grch = ?
                    """,
                    (row[1], row[0]),
                )
            cursor.execute(
                "UPDATE `db`.`source` SET current_ucschg = ucschg "
                "WHERE current_ucschg IS NULL"
            )

            # check for any source with an unrecognized GRCh build
            mismatch = False
            for row in cursor.execute(
                "SELECT source, grch, ucschg FROM `db`.`source` "
                "WHERE (grch IS NULL) != (ucschg IS NULL)"
            ):
                self.log(
                    "WARNING: unrecognized genome build for '%s' (NCBI GRCh%s, UCSC hg%s)\n"  # noqa: E501
                    % (row[0], (row[1] or "?"), (row[2] or "?"))
                )
                mismatch = True
            if mismatch:
                self.log(
                    "WARNING: database may contain incomparable genome positions!\n"  # noqa: E501
                )

            # check all sources' UCSChg build versions and set the latest as
            # the target
            hgSources = collections.defaultdict(set)
            for row in cursor.execute(
                "SELECT source_id, current_ucschg "
                "FROM `db`.`source` "
                "WHERE current_ucschg IS NOT NULL"
            ):
                hgSources[row[1]].add(row[0])
            if hgSources:
                targetHG = max(hgSources)
                self.log(
                    "database genome build: GRCh%s / UCSChg%s\n"
                    % (ucscGRC.get(targetHG, "?"), targetHG)
                )
                targetUpdated = (
                    self._loki.getDatabaseSetting("ucschg", int) != targetHG
                )  # noqa: E501
                self._loki.setDatabaseSetting("ucschg", targetHG)

            # liftOver sources with old build versions, if there are any
            if len(hgSources) > 1:
                locusSources = set(
                    row[0]
                    for row in cursor.execute(
                        "SELECT DISTINCT source_id FROM `db`.`snp_locus`"
                    )
                )
                regionSources = set(
                    row[0]
                    for row in cursor.execute(
                        """
                        SELECT DISTINCT source_id
                        FROM `db`.`biopolymer_region`
                        """
                    )  # noqa: E501
                )
                chainsUpdated = (
                    "grch_ucschg" in self._tablesUpdated
                    or "chain" in self._tablesUpdated
                    or "chain_data" in self._tablesUpdated
                )
                for oldHG in sorted(hgSources):
                    if oldHG == targetHG:
                        continue
                    if not self._loki.hasLiftOverChains(oldHG, targetHG):
                        self.log(
                            "ERROR: no chains available to lift hg%d to hg%d\n"
                            % (oldHG, targetHG)
                        )
                        continue

                    if (
                        targetUpdated
                        or chainsUpdated
                        or "snp_locus" in self._tablesUpdated
                    ):
                        sourceIDs = hgSources[oldHG] & locusSources
                        if sourceIDs:
                            self.liftOverSNPLoci(oldHG, targetHG, sourceIDs)
                    if (
                        targetUpdated
                        or chainsUpdated
                        or "biopolymer_region" in self._tablesUpdated
                    ):
                        sourceIDs = hgSources[oldHG] & regionSources
                        if sourceIDs:
                            self.liftOverRegions(oldHG, targetHG, sourceIDs)

                    sql = (
                        "UPDATE `db`.`source` SET current_ucschg = %d "
                        "WHERE source_id = ?" % targetHG
                    )
                    cursor.executemany(
                        sql, ((sourceID,) for sourceID in hgSources[oldHG])
                    )
                # foreach old build
            # if any old builds

            # post-process as needed
            # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if "snp_merge" in self._tablesUpdated:
                self.cleanupSNPMerges()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if (
                "snp_merge" in self._tablesUpdated
                or "snp_locus" in self._tablesUpdated  # noqa: E501
            ):  # noqa: E501
                self.updateMergedSNPLoci()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if "snp_locus" in self._tablesUpdated:
                self.cleanupSNPLoci()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if (
                "snp_merge" in self._tablesUpdated
                or "snp_entrez_role" in self._tablesUpdated
            ):
                self.updateMergedSNPEntrezRoles()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if "snp_entrez_role" in self._tablesUpdated:
                self.cleanupSNPEntrezRoles()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if (
                "snp_merge" in self._tablesUpdated
                or "gwas" in self._tablesUpdated  # noqa: E501
            ):  # noqa: E501
                self.updateMergedGWASAnnotations()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if (
                "biopolymer_name" in self._tablesUpdated
                or "biopolymer_name_name" in self._tablesUpdated
            ):
                self.resolveBiopolymerNames()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if (
                "biopolymer_name" in self._tablesUpdated
                or "snp_entrez_role" in self._tablesUpdated
            ):
                self.resolveSNPBiopolymerRoles()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if (
                "biopolymer_name" in self._tablesUpdated
                or "group_member_name" in self._tablesUpdated
            ):
                self.resolveGroupMembers()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
            if "biopolymer_region" in self._tablesUpdated:
                self.updateBiopolymerZones()
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501

            # reindex all remaining tables
            if self._tablesDeindexed:
                self._loki.createDatabaseIndices(
                    None, "db", self._tablesDeindexed
                )  # noqa: E501
            if self._tablesUpdated:
                self._loki.setDatabaseSetting("optimized", 0)
            self.log("updating database completed\n")
        except:  # noqa: E722
            excType, excVal, excTrace = sys.exc_info()
            while self.logPop() > logIndent:
                pass
            self.logPush("ERROR: failed to update the database\n")
            if excTrace:
                for line in traceback.format_list(
                    traceback.extract_tb(excTrace)[-1:]
                ):  # noqa: E501
                    self.log(line)
            for line in traceback.format_exception_only(excType, excVal):
                self.log(line)
            self.logPop()
            cursor.execute(
                "ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase'"
            )  # noqa: E501
        finally:
            cursor.execute("RELEASE SAVEPOINT 'updateDatabase'")
            self._updating = False
            self._tablesUpdated = set()
            self._tablesDeindexed = set()
            os.chdir(iwd)
        # try/except/finally

        # report and return
        if srcErrors:
            self.logPush("WARNING: data from these sources was not updated:\n")
            for srcName in sorted(srcErrors):
                self.log("%s\n" % srcName)
            self.logPop()
            return False
        return True
