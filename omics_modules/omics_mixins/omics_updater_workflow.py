# #################################################
# UPDATER WORKFLOW MIXIN
# #################################################
import collections
import os
import sys
import traceback
import shutil
from threading import Thread


class UpdaterWorkflowMixin:

    def workprocess(
        self,
        sources=None,
        sourceOptions=None,
        cacheOnly=False,
        forceUpdate=False,
    ):

        # PREPARATION PHASE
        # =====================================================================

        # check for extraneous options
        # self.logger.log("[INFO] Dentro do workprocess.")
        # self.logger.log("Preparing for update ...")
        srcSet = self.attachSourceModules(sources)
        srcOpts = sourceOptions or {}
        for srcName in srcOpts.keys():
            if srcName not in srcSet:
                self.logger.log(
                    "WARNING: not updating from source '%s' for which options were supplied\n"  # noqa: E501
                    % srcName
                )
        self.logger.log(
            "Preparing for update completed\n")


        # update all specified sources
        iwd = os.path.abspath(os.getcwd())
        self._updating = True
        self._tablesUpdated = set()
        self._tablesDeindexed = set()
        srcErrors = set()
        
        # cursor = self._db.cursor()
        cursor = self._database.get_session()
        # cursor.execute("SAVEPOINT 'updateDatabase'")
        

        # prepare sources options from database
        self.logger.log(
            "Preparing sources options from database ..."           
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
                self.logger.log(
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
                    self.logger.log("%s = %s\n" % (opt, options[opt]))
                self.logger.log(
                    "Adding %s options completed\n" % srcName
                )  # noqa: E501
            # temp for now but should replace options everywhere below
            self._sourceOptions[srcName] = options
        self.logger.log(
            "Preparing sources options from database completed\n"
            
        )

        # DOWNLOAD PHASE
        # =====================================================================
        # 📡 Start Parallel Download and Hashing

        if self.skipDownload:
            self.logger.log(
                "Set to skip download, skipping the download and hashing"
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
                self.logger.log(
                    srcName + " rejoined main thread"                        
                )

        # PROCESSING PHASE
        # =====================================================================
        # Return the flow of the code to the main thread

        for srcName in self.srcSetsToDownload:
            srcObj = self._sourceObjects[srcName]
            srcID = srcObj.getSourceID()
            options = self._sourceOptions[srcName]
            path = os.path.join(iwd, srcName)


            # 🚨 Start Update Method from Source System
            # Call the update function of the source
            srcObj.update(options, path)  

            # REMOVE FILES PHASE
            # =============================================================

            # remove subdirectory to free up some space
            if not self.keepDownload:
                shutil.rmtree(path)
