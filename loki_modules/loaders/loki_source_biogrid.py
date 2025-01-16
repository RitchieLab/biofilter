import zipfile
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
        self.log("deleting old records from the database ...\n")
        self.deleteAll()
        self.log("deleting old records from the database completed\n")

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
        self.log("verifying archive file ...\n")
        pairLabels = dict()
        empty = tuple()
        with zipfile.ZipFile(
            path + "/BIOGRID-ORGANISM-LATEST.tab2.zip", "r"
        ) as assocZip:
            err = assocZip.testzip()
            if err:
                self.log(" ERROR\n")
                self.log("CRC failed for %s\n" % err)
                return False
            self.log("verifying archive file completed\n")
            self.log("processing gene interactions ...\n")
            for info in assocZip.infolist():
                if info.filename.find("Homo_sapiens") >= 0:
                    assocFile = assocZip.open(info, "r")
                    header = assocFile.__next__().rstrip()
                    observedHeaders = {
                        "#BioGRID Interaction ID\tEntrez Gene Interactor A\tEntrez Gene Interactor B\tBioGRID ID Interactor A\tBioGRID ID Interactor B\tSystematic Name Interactor A\tSystematic Name Interactor B\tOfficial Symbol Interactor A\tOfficial Symbol Interactor B\tSynonymns Interactor A\tSynonyms Interactor B\tExperimental System\tExperimental System Type\tAuthor\tPubmed ID\tOrganism Interactor A\tOrganism Interactor B",  # "\tThroughput\tScore\tModification\tPhenotypes\tQualifications\tTags\tSource Database",  # noqa E501
                        "#BioGRID Interaction ID\tEntrez Gene Interactor A\tEntrez Gene Interactor B\tBioGRID ID Interactor A\tBioGRID ID Interactor B\tSystematic Name Interactor A\tSystematic Name Interactor B\tOfficial Symbol Interactor A\tOfficial Symbol Interactor B\tSynonyms Interactor A\tSynonyms Interactor B\tExperimental System\tExperimental System Type\tAuthor\tPubmed ID\tOrganism Interactor A\tOrganism Interactor B",  # "\tThroughput\tScore\tModification\tPhenotypes\tQualifications\tTags\tSource Database",  # noqa E501
                    }
                    if not max(
                        header.decode().startswith(obsHdr)
                        for obsHdr in observedHeaders  # noqa: E501
                    ):
                        self.log(" ERROR\n")
                        self.log(
                            "unrecognized file header in '%s': %s\n"
                            % (info.filename, header)
                        )
                        return False
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
        numAssoc = len(pairLabels)
        numGene = len(
            set(pair[0] for pair in pairLabels)
            | set(pair[1] for pair in pairLabels)  # noqa E501
        )
        numName = sum(len(pairLabels[pair]) for pair in pairLabels)
        self.log(
            "processing gene interactions completed: %d interactions (%d genes), %d pair identifiers\n"  # noqa E501
            % (numAssoc, numGene, numName)
        )

        # store interaction groups
        self.log("writing interaction pairs to the database ...\n")
        listPair = pairLabels.keys()
        listGID = self.addTypedGroups(
            typeID["interaction"],
            (
                ("biogrid:%s" % min(pairLabels[pair]), None)
                for pair in listPair  # noqa E501
            ),
        )
        pairGID = dict(zip(listPair, listGID))
        self.log("writing interaction pairs to the database completed\n")

        # store interaction labels
        listLabels = []
        for pair in listPair:
            listLabels.extend(
                (pairGID[pair], label) for label in pairLabels[pair]
            )  # noqa E501
        self.log("writing interaction names to the database ...\n")
        self.addGroupNamespacedNames(namespaceID["biogrid_id"], listLabels)
        self.log("writing interaction names to the database completed\n")

        # store gene interactions
        self.log("writing gene interactions to the database ...\n")
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
        self.log("writing gene interactions to the database completed\n")


# TODO: if there's any value in trying to identify pseudo-pathways
"""
self.log("identifying implied networks ...")
geneAssoc = dict()
for pair in listPair:
    if pair[0] not in geneAssoc:
        geneAssoc[pair[0]] = set()
    geneAssoc[pair[0]].add(pair[1])
    if pair[1] not in geneAssoc:
        geneAssoc[pair[1]] = set()
    geneAssoc[pair[1]].add(pair[0])
listPath = self.findMaximalCliques(geneAssoc)
numAssoc = sum(len(path) for path in listPath)
numGene = len(geneAssoc)
numGroup = len(listPath)
self.log(
" OK: %d associations (%d genes in %d groups)\n"
% (numAssoc,numGene,numGroup))
"""
