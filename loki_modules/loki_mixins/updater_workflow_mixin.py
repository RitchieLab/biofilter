# #################################################
# UPDATER WORKFLOW MIXIN
# #################################################
import collections
import os
import sys
import traceback
import shutil
from threading import Thread
import logging

# 🏗️ Improvements to this Mixin / Splitting the methods by PHASE
# def updaterWorkflow(self):
#     self._Preparation()
#     self._Download()
#     self._ProcessAndIngestData()
#     self._Metadata()
#     self._RemoveDownload()
#     self._PostProcessingOperations()


class UpdaterWorkflowMixin:

    def updateDatabase(
        self,
        sources=None,
        sourceOptions=None,
        cacheOnly=False,
        forceUpdate=False,
    ):
        if self._updating:
            raise Exception("_updating set before updateDatabase()")
        self._loki.testDatabaseWriteable()
        if self._loki.getDatabaseSetting("finalized", int):
            raise Exception("cannot update a finalized database")

        # PREPARATION PHASE
        # =====================================================================

        # check for extraneous options
        self.log("Preparing for update ...", level=logging.INFO, indent=0)
        srcSet = self.attachSourceModules(sources)
        srcOpts = sourceOptions or {}
        for srcName in srcOpts.keys():
            if srcName not in srcSet:
                self.log(
                    "WARNING: not updating from source '%s' for which options were supplied\n"  # noqa: E501
                    % srcName,
                    level=logging.WARNING,
                    indent=2,
                )
        self.log(
            "Preparing for update completed\n", level=logging.INFO, indent=0
        )  # noqa: E501

        # update all specified sources
        iwd = os.path.abspath(os.getcwd())
        self._updating = True
        self._tablesUpdated = set()
        self._tablesDeindexed = set()
        srcErrors = set()
        cursor = self._db.cursor()
        cursor.execute("SAVEPOINT 'updateDatabase'")
        try:

            # prepare sources options from database
            self.log(
                "Preparing sources options from database ...",
                level=logging.INFO,
                indent=0,
            )
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
                    self.log(
                        "%s %s options ...\n"
                        % (
                            (
                                "validating"
                                if (srcName in srcOpts)
                                else "loading prior"
                            ),  # noqa: E501
                            srcName,
                        ),
                        level=logging.INFO,
                        indent=2,
                    )
                msg = srcObj.validateOptions(options)
                if msg is not True:
                    raise Exception(msg)
                if optionsList:
                    for opt in optionsList:
                        self.log("%s = %s\n" % (opt, options[opt]))
                    self.log(
                        "Adding %s options completed\n" % srcName,
                        level=logging.INFO,
                        indent=2,
                    )  # noqa: E501
                # temp for now but should replace options everywhere below
                self._sourceOptions[srcName] = options
            self.log(
                "Preparing sources options from database completed\n",
                level=logging.INFO,
                indent=0,
            )

            # DOWNLOAD PHASE
            # =====================================================================
            # 📡 Start Parallel Download and Hashing

            if self.skipDownload:
                self.log(
                    "Set to skip download, skipping the download and hashing",
                    level=logging.WARNING,
                    indent=0,
                )
                self.srcSetsToDownload = sorted(srcSet)
                # Preciso ir nas pastas e calcular o hash dos arquivos

            else:
                downloadAndHashThreads = {}
                self.srcSetsToDownload = sorted(srcSet)
                # Create buffer to run in parallel
                for srcName in self.srcSetsToDownload:
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

                # Wait for all download and hash threads to finish
                for srcName in downloadAndHashThreads.keys():
                    downloadAndHashThreads[srcName].join()
                    self.log(
                        srcName + " rejoined main thread",
                        level=logging.INFO,
                        indent=0,
                    )

            # ----------------------------------------------
            # Check if only download is set
            if self.onlyDownload:
                self.log(
                    "Set to only download, skipping the rest of the update",
                    level=logging.WARNING,
                    indent=0,
                )
                return True
            # NOTE When --only_download is set, the code will return here
            # NOTE Will keep all files independent of keep_downloads

            # PROCESSING PHASE
            # =====================================================================
            # Return the flow of the code to the main thread

            for srcName in self.srcSetsToDownload:
                srcObj = self._sourceObjects[srcName]
                srcID = srcObj.getSourceID()
                options = self._sourceOptions[srcName]
                path = os.path.join(iwd, srcName)

                cursor.execute("SAVEPOINT 'updateDatabase_%s'" % (srcName,))
                self.log(
                    "Savepoint for %s" % srcName,
                    level=logging.INFO,
                    indent=0,
                )

                try:
                    # compare current loader version, options and file
                    # metadata to the last update
                    skip = not forceUpdate
                    last = "?"
                    # New Loader Version
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
                    # New Options
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
                    # New Files (Size and MD5 difference)
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
                            # Get the file size and MD5 hash directly from
                            # _filehash
                            file_size, file_md5 = row[1], row[2]
                            # Check if there is any file in _filehash with the
                            # same parameters

                            # TODO : Analisar o que ocorre quando ha mais que
                            # um arquivo no mesmo loader
                            if self.skipDownload:
                                self.fileHash(row[0])

                            match_found = any(
                                file_hash_data[1] == file_size
                                and file_hash_data[3] == file_md5  # noqa: E501
                                for file_hash_data in self._filehash.values()
                            )
                            # Update the state of `skip`
                            skip = skip and match_found
                        # Apply to multiple files
                        skip = skip and (n == len(self._filehash))

                    # skip the update if the current loader and all source
                    # file versions match the last update
                    if skip:
                        self.log(
                            "Skipping %s update, no data or software changes since %s"  # noqa: E501
                            % (srcName, last),
                            level=logging.WARNING,
                            indent=0,
                        )
                    else:
                        # process new files (or old files with a new loader)
                        self.log(
                            "Starting the processing of %s data ..." % srcName,
                            level=logging.INFO,
                            indent=0,
                        )

                        cursor.execute(
                            """
                            DELETE FROM `db`.`warning`
                            WHERE source_id = ?
                            """,
                            (srcID,),  # noqa: E501
                        )
                        self.log(
                            "Deleted warning table for %s data ..." % srcName,
                            level=logging.INFO,
                            indent=2,
                        )

                        # 🚨 Start Update Method from Source System
                        # Call the update function of the source
                        srcObj.update(options, path)

                        # METADATA PHASE
                        # =====================================================
                        # update the source metadata

                        cursor.execute(
                            """
                            UPDATE `db`.`source`
                            SET updated = DATETIME('now'),
                                version = ?,
                                last_status = ?
                            WHERE source_id = ?
                            """,
                            (
                                srcObj.getVersionString(),
                                1,
                                srcID,
                            ),  # 1 = success / 0 = error
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

                        self.log(
                            "Metadata updated for %s" % srcName,
                            level=logging.INFO,
                            indent=0,
                        )

                        self.log(
                            "Processing %s data completed" % srcName,
                            level=logging.INFO,
                            indent=0,
                        )
                    # if skip
                    # 🚧 Preciso garantir que todos os Source Systems se derem erro vao vir para esse exeption e nao ter um raise

                except Exception as e:
                    srcErrors.add(srcName)
                    excType, excVal, excTrace = sys.exc_info()
                    # while self.logPop() > 1:
                    #     pass
                    self.log(
                        "ERROR: failed to update %s\n" % (srcName,),
                        level=logging.ERROR,
                        indent=1,
                    )
                    self.log_exception(e)
                    if excTrace:
                        for line in traceback.format_list(
                            traceback.extract_tb(excTrace)[-1:]
                        ):
                            self.log(
                                line,
                                level=logging.ERROR,
                                indent=1,
                            )
                    for line in traceback.format_exception_only(
                        excType, excVal
                    ):  # noqa E501
                        self.log(
                            line,
                            level=logging.ERROR,
                            indent=1,
                        )
                    cursor.execute(
                        "ROLLBACK TRANSACTION TO SAVEPOINT 'updateDatabase_%s'"
                        % (srcName,)
                    )
                    self.log(
                        "Rollback Savepoint for %s\n" % srcName,
                        level=logging.WARNING,
                        indent=0,
                    )

                    # Add error status on Source and Warning Table
                    cursor.execute(
                        """
                        UPDATE `db`.`source`
                        SET last_status = ?
                        WHERE source_id = ?
                        """,
                        (0, srcID),
                    )
                    cursor.execute(
                        """
                        INSERT INTO `db`.`warning`
                        (source_id, message)
                        VALUES (?, ?)
                        """,
                        (srcID, str(e)),
                    )
                    self.log(
                        "Added error status on Source and Warning Table for %s\n"
                        % srcName,
                        level=logging.ERROR,
                        indent=0,
                    )

                finally:
                    cursor.execute(
                        "RELEASE SAVEPOINT 'updateDatabase_%s'" % (srcName,)
                    )  # noqa: E501
                    self.log(
                        "Released Savepoint for %s\n" % srcName,
                        level=logging.CRITICAL,
                        indent=0,
                    )
                # try/except/finally

                # REMOVE FILES PHASE
                # =============================================================

                # remove subdirectory to free up some space
                if not self.keepDownload:
                    shutil.rmtree(path)
                    self.log(
                        f"Removed {srcName} download directory",
                        level=logging.INFO,
                        indent=0,
                    )
                else:
                    self.log(
                        f"Kept {srcName} download directory",
                        level=logging.INFO,
                        indent=0,
                    )
            # foreach source

            # POS INGESTION DATA PHASE
            # =====================================================================

            # pull the latest GRCh/UCSChg conversions
            #   http://genome.ucsc.edu/FAQ/FAQreleases.html
            #   http://genome.ucsc.edu/goldenPath/releaseLog.html
            # TODO: find a better machine-readable source for this data

            # 🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨
            # Eu preciso entender o que é isso e como funciona, pois realiza opreações no banco de dados
            # apos a atualização dos dados e esta consumindo muita memoria e tempo.
            
            pos_processamento = True
            if pos_processamento:
                # 📍INICIO DO BLOCLO 1: Objetivo eh verificar se temos campos nulos em grch e ucschg na table Source, e alinhar com o que esta em UCSC site
                #  Step 1: baixar o site do UCSC e extrair as informações de grch e ucschg
                #  Step 2: Atualizar a tabela grch_ucschg com as informações do UCSC
                #  Step 3: Atualizar a tabela Source com as informações do UCSC
                #  Step 4: Verificar se algum valor da tabela Source esta fora do range do UCSC

                if not cacheOnly:  # If False it WILL RUN  / Move to a other method
                    self.log(
                        "updating GRCh:UCSChg genome build identities ...",
                        level=logging.INFO,
                        indent=0,
                    )
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
                        "updating GRCh:UCSChg genome build identities completed\n",
                        level=logging.INFO,
                        indent=0,  # noqa: E501
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
                        % (row[0], (row[1] or "?"), (row[2] or "?")),
                        level=logging.WARNING,
                        indent=0,
                    )
                    mismatch = True
                if mismatch:
                    self.log(
                        "WARNING: database may contain incomparable genome positions!\n",  # noqa: E501
                        level=logging.WARNING,
                        indent=0,
                    )

                # 📍FIM DO BLOCLO 1


                # 📍INICIO DO BLOCLO 2: Se algum Source for diferente do definito, realizar um liftover
                # 🚨 Usa a Cahin table para isso (talvez ela deva ser a primeira a ser atualizada)

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
                        % (ucscGRC.get(targetHG, "?"), targetHG),
                        level=logging.INFO,
                        indent=0,
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

                # 📍FIM DO BLOCLO 2 (liftover)

                # 📍INICIO DO BLOCO 3

                # post-process as needed
                # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
                
                # NOTE esse method elimina rsMerge repeatidos da snp_merge table
                if "snp_merge" in self._tablesUpdated:
                    self.cleanupSNPMerges()
                    # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501

                # NOTE esse method olha na snp_merged e insere novos registros na snp_locus com base em snp da snp_locus que foram Merged
                # Resumo, ele ira criar novos registros na snp_locus com base em snp da snp_merged
                if (
                    "snp_merge" in self._tablesUpdated
                    or "snp_locus" in self._tablesUpdated  # noqa: E501
                ):  # noqa: E501
                    self.updateMergedSNPLoci()
                    # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
                
                # NOTE Elimina registros da snp_locus (mantem apenas um para o grupo snp, chr e pos)
                if "snp_locus" in self._tablesUpdated:
                    self.cleanupSNPLoci()
                    # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
                
                # NOTE MESMO PROCESSO DO ANTERIOR, POREM PARA A TABELA snp_entrez_role
                if (
                    "snp_merge" in self._tablesUpdated
                    or "snp_entrez_role" in self._tablesUpdated
                ):
                    self.updateMergedSNPEntrezRoles()
                    # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
                
                if "snp_entrez_role" in self._tablesUpdated:
                    self.cleanupSNPEntrezRoles()
                    # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
                
                # NOTE Duplica registro na GWAS (usando o rsMerge)
                if (
                    "snp_merge" in self._tablesUpdated
                    or "gwas" in self._tablesUpdated  # noqa: E501
                ):  # noqa: E501
                    self.updateMergedGWASAnnotations()
                    # self.log("MEMORY: %d bytes (%d peak)\n" % self._loki.getDatabaseMemoryUsage()) #DEBUG  # noqa: E501
                
                # NOTE PRocessamento para o Biopolymer
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
                self.log(
                    "updating database completed\n",
                    level=logging.INFO,
                    indent=0,
                )

        # FIM DO POS PROCESSAMENTO

        except Exception as e:
            self.log_exception(e)
            excType, excVal, excTrace = sys.exc_info()
            # while self.logPop() > 1:
            #     pass
            self.log(
                "ERROR: failed to update the database\n",
                level=logging.ERROR,
                indent=1,
            )

            if excTrace:
                for line in traceback.format_list(
                    traceback.extract_tb(excTrace)[-1:]
                ):  # noqa: E501
                    self.log(
                        line,
                        level=logging.ERROR,
                        indent=1,
                    )
            for line in traceback.format_exception_only(excType, excVal):
                self.log(
                    line,
                    level=logging.ERROR,
                    indent=1,
                )
            # self.logPop()
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
            self.log(
                "WARNING: data from these sources was not updated:\n",
                level=logging.WARNING,
                indent=0,
            )
            for srcName in sorted(srcErrors):
                self.log(
                    "%s\n" % srcName,
                    level=logging.WARNING,
                    indent=1,
                )
            return False
        return True
