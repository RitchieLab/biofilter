import zipfile
import logging
import psutil
import time
from loki_modules import loki_source


class Source_biogrid(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        # download the latest source files
        self.downloadFilesFromHTTP(
            "downloads.thebiogrid.org",
            {
                path
                + "/BIOGRID-ORGANISM-LATEST.tab2.zip": "/Download/BioGRID/Latest-Release/BIOGRID-ORGANISM-LATEST.tab2.zip",  # noqa: E501
            },
        )

        return [path + "/BIOGRID-ORGANISM-LATEST.tab2.zip"]

    def update(self, options, path):
        # clear out all old data from this source
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"BioGRID - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "BioGRID - Starting deletion of old records from the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "BioGRID - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [
                ("biogrid_id", 0),
                ("symbol", 0),
                ("entrez_gid", 0),
            ]
        )
        typeID = self.addTypes(
            [
                ("interaction",),
                ("gene",),
            ]
        )

        # process associations
        self.log(
            "BioGRID - Starting verification of archive file ...",
            level=logging.INFO,
            indent=2,
        )
        pairLabels = dict()
        empty = tuple()
        with zipfile.ZipFile(
            path + "/BIOGRID-ORGANISM-LATEST.tab2.zip", "r"
        ) as assocZip:
            # verify the archive file
            err = assocZip.testzip()
            if err:
                # if the archive file is corrupted stop processing
                self.log(
                    "BioGRID - CRC failed for %s" % err,
                    level=logging.ERROR,
                    indent=2,
                )
                self.log(
                    "BioGRID - Archive file failed",
                    level=logging.ERROR,
                    indent=2,
                )
                return False
            self.log("BioGRID - File is valid", level=logging.INFO, indent=2)

            # process gene interactions
            self.log(
                "BioGRID - Starting the processing of gene interactions ...",
                level=logging.INFO,
                indent=2,
            )  # noqa E501
            for info in assocZip.infolist():
                if info.filename.find("Homo_sapiens") >= 0:
                    assocFile = assocZip.open(info, "r")

                    # check file header
                    header = assocFile.__next__().rstrip()
                    observedHeaders = {
                        "#BioGRID Interaction ID\tEntrez Gene Interactor A\tEntrez Gene Interactor B\tBioGRID ID Interactor A\tBioGRID ID Interactor B\tSystematic Name Interactor A\tSystematic Name Interactor B\tOfficial Symbol Interactor A\tOfficial Symbol Interactor B\tSynonymns Interactor A\tSynonyms Interactor B\tExperimental System\tExperimental System Type\tAuthor\tPubmed ID\tOrganism Interactor A\tOrganism Interactor B",  # "\tThroughput\tScore\tModification\tPhenotypes\tQualifications\tTags\tSource Database",  # noqa E501
                        "#BioGRID Interaction ID\tEntrez Gene Interactor A\tEntrez Gene Interactor B\tBioGRID ID Interactor A\tBioGRID ID Interactor B\tSystematic Name Interactor A\tSystematic Name Interactor B\tOfficial Symbol Interactor A\tOfficial Symbol Interactor B\tSynonyms Interactor A\tSynonyms Interactor B\tExperimental System\tExperimental System Type\tAuthor\tPubmed ID\tOrganism Interactor A\tOrganism Interactor B",  # "\tThroughput\tScore\tModification\tPhenotypes\tQualifications\tTags\tSource Database",  # noqa E501
                    }
                    if not max(
                        header.decode().startswith(obsHdr)
                        for obsHdr in observedHeaders  # noqa: E501
                    ):
                        self.log(
                            "BioGRID - Unrecognized file header in '%s': %s"
                            % (info.filename, header),
                            level=logging.ERROR,
                            indent=2,
                        )
                        self.log(
                            "BioGRID - Archive file failed",
                            level=logging.ERROR,
                            indent=2,
                        )
                        return False

                    # Read the file data
                    for line in assocFile:
                        line = line.decode()
                        words = line.split("\t")
                        if words[1] == "-" or words[2] == "-":
                            continue
                        bgID = int(words[0])
                        entrezID1 = int(words[1])
                        entrezID2 = int(words[2])
                        syst1 = words[5] if words[5] != "-" else None
                        syst2 = words[6] if words[6] != "-" else None
                        gene1 = words[7]
                        gene2 = words[8]
                        aliases1 = (
                            words[9].split("|") if words[9] != "-" else empty
                        )  # noqa E501
                        aliases2 = (
                            words[10].split("|") if words[10] != "-" else empty
                        )  # noqa E501
                        tax1 = words[15]
                        tax2 = words[16]

                        if tax1 == "9606" and tax2 == "9606":
                            member1 = (entrezID1, gene1, syst1) + tuple(
                                aliases1
                            )  # noqa E501
                            member2 = (entrezID2, gene2, syst2) + tuple(
                                aliases2
                            )  # noqa E501
                            if member1 != member2:
                                pair = (member1, member2)
                                if pair not in pairLabels:
                                    pairLabels[pair] = set()
                                pairLabels[pair].add(bgID)
                        # if interaction is ok
                    # foreach line in assocFile
                    assocFile.close()
                # if Homo_sapiens file
            # foreach file in assocZip
        # with assocZip

        # show statistics
        numAssoc = len(pairLabels)
        numGene = len(
            set(pair[0] for pair in pairLabels)
            | set(pair[1] for pair in pairLabels)  # noqa E501
        )
        numName = sum(len(pairLabels[pair]) for pair in pairLabels)
        self.log(
            "BioGRID - Procesing gene interactions completed: %d interactions (%d genes), %d pair identifiers"  # noqa E501
            % (numAssoc, numGene, numName),
            level=logging.INFO,
            indent=2,
        )

        # store interaction groups
        self.log(
            "BioGRID - Starting the writing interaction pairs to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        listPair = pairLabels.keys()
        listGID = self.addTypedGroups(
            typeID["interaction"],
            (
                ("biogrid:%s" % min(pairLabels[pair]), None)
                for pair in listPair  # noqa E501
            ),
        )
        pairGID = dict(zip(listPair, listGID))
        self.log(
            "BioGRID - Writing interaction pairs to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # store interaction labels
        self.log(
            "BioGRID - Starting the writing interaction names to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        listLabels = []
        for pair in listPair:
            listLabels.extend(
                (pairGID[pair], label) for label in pairLabels[pair]
            )  # noqa E501
        self.addGroupNamespacedNames(namespaceID["biogrid_id"], listLabels)
        self.log(
            "BioGRID - Writing interaction names to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # store gene interactions
        self.log(
            "BioGRID - Starting the writing gene interactions to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        nsAssoc = {
            "symbol": set(),
            "entrez_gid": set(),
        }
        numAssoc = 0
        for pair in pairLabels:
            numAssoc += 1
            nsAssoc["entrez_gid"].add((pairGID[pair], numAssoc, pair[0][0]))
            for n in range(1, len(pair[0])):
                nsAssoc["symbol"].add((pairGID[pair], numAssoc, pair[0][n]))

            numAssoc += 1
            nsAssoc["entrez_gid"].add((pairGID[pair], numAssoc, pair[1][0]))
            for n in range(1, len(pair[1])):
                nsAssoc["symbol"].add((pairGID[pair], numAssoc, pair[1][n]))
        for ns in nsAssoc:
            self.addGroupMemberTypedNamespacedNames(
                typeID["gene"], namespaceID[ns], nsAssoc[ns]
            )
        self.log(
            "BioGRID - Writing gene interactions to the database completed",
            level=logging.INFO,
            indent=2,
        )

        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"BioGRID - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"BioGRID - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )
