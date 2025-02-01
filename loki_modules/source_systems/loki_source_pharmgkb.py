import zipfile
import logging
import psutil
import time
from loki_modules import loki_source


class Source_pharmgkb(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        self.downloadFilesFromHTTPS(
            "api.pharmgkb.org",
            {
                path + "/genes.zip": "/v1/download/file/data/genes.zip",
                path
                + "/pathways-tsv.zip": "/v1/download/file/data/pathways-tsv.zip",  # noqa E501
            },
        )

        return [path + "/genes.zip", path + "/pathways-tsv.zip"]

    def update(self, options, path):
        # clear out all old data from this source
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"PharmGKB - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "PharmGKB - Starting deletion of old records from the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "PharmGKB - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [
                ("pharmgkb_id", 0),
                ("pathway", 0),
                ("pharmgkb_gid", 0),
                ("symbol", 0),
                ("entrez_gid", 0),
                ("refseq_gid", 0),
                ("refseq_pid", 1),
                ("ensembl_gid", 0),
                ("ensembl_pid", 1),
                ("hgnc_id", 0),
                ("uniprot_gid", 0),
                ("uniprot_pid", 1),
            ]
        )
        typeID = self.addTypes(
            [
                ("gene",),
                ("pathway",),
            ]
        )

        # process gene names
        self.log(
            "PharmGKB - Starting the verifying gene name archive file ...",
            level=logging.INFO,
            indent=2,
        )
        setNames = set()
        empty = tuple()
        with zipfile.ZipFile(path + "/genes.zip", "r") as geneZip:
            err = geneZip.testzip()
            if err:
                self.log(
                    "PharmGKB - CRC failed for %s" % err,
                    level=logging.ERROR,
                    indent=2,
                )
                return False
            self.log(
                "PharmGKB - Verifying gene name archive file completed",
                level=logging.INFO,
                indent=2,
            )
            self.log(
                "PharmGKB - Starting the processing gene names ...",
                level=logging.INFO,
                indent=2,
            )
            xrefNS = {
                "entrezGene": ("entrez_gid",),
                "refSeqDna": ("refseq_gid",),
                "refSeqRna": ("refseq_gid",),
                "refSeqProtein": ("refseq_pid",),
                "ensembl": ("ensembl_gid", "ensembl_pid"),
                "hgnc": ("hgnc_id",),
                "uniProtKb": ("uniprot_gid", "uniprot_pid"),
            }
            for info in geneZip.infolist():
                if info.filename == "genes.tsv":
                    geneFile = geneZip.open(info, "r")
                    header = geneFile.__next__().rstrip()
                    if header.decode().startswith(
                        "PharmGKB Accession Id	Entrez Id	Ensembl Id	Name	Symbol	Alternate Names	Alternate Symbols	Is VIP	Has Variant Annotation	Cross-references"  # noqa E501
                    ):
                        new2 = 0
                    elif header.decode().startswith(
                        "PharmGKB Accession Id	NCBI Gene ID	HGNC ID	Ensembl Id	Name	Symbol	Alternate Names	Alternate Symbols	Is VIP	Has Variant Annotation	Cross-references"  # noqa E501
                    ):
                        new2 = 1
                    else:
                        self.log(
                            "PharmGKB - Unrecognized file header in '%s': %s"
                            % (info.filename, header),
                            level=logging.ERROR,
                            indent=2,
                        )
                        raise Exception(
                            "ERROR: unrecognized file header in '%s': %s"
                            % (info.filename, header)
                        )

                    for line in geneFile:
                        words = line.decode("latin-1").split("\t")
                        pgkbID = words[0]
                        entrezID = words[1]
                        ensemblID = words[2 + new2]
                        symbol = words[4 + new2]
                        aliases = (
                            words[6 + new2].split(",")
                            if words[6 + new2] != ""
                            else empty
                        )
                        xrefs = (
                            words[9 + new2].strip(", \r\n").split(",")
                            if words[9 + new2] != ""
                            else empty
                        )

                        if entrezID:
                            setNames.add(
                                (namespaceID["entrez_gid"], entrezID, pgkbID)
                            )  # noqa E501
                        if ensemblID:
                            setNames.add(
                                (namespaceID["ensembl_gid"], ensemblID, pgkbID)
                            )
                            setNames.add(
                                (namespaceID["ensembl_pid"], ensemblID, pgkbID)
                            )
                        if symbol:
                            setNames.add(
                                (namespaceID["symbol"], symbol, pgkbID)
                            )  # noqa E501
                        for alias in aliases:
                            setNames.add(
                                (
                                    namespaceID["symbol"],
                                    alias.strip('" '),
                                    pgkbID,
                                )  # noqa E501
                            )
                        for xref in xrefs:
                            try:
                                xrefDB, xrefID = xref.split(":", 1)
                                if xrefDB in xrefNS:
                                    for ns in xrefNS[xrefDB]:
                                        setNames.add(
                                            (namespaceID[ns], xrefID, pgkbID)
                                        )  # noqa E501
                            except ValueError:
                                pass
                    # foreach line in geneFile
                    geneFile.close()
                # if genes.tsv
            # foreach file in geneZip
        # with geneZip
        numIDs = len(set(n[2] for n in setNames))
        self.log(
            "PharmGKB - Processing gene names completed: %d identifiers (%d references)"  # noqa E501
            % (numIDs, len(setNames)),
            level=logging.INFO,
            indent=2,
        )

        # store gene names
        self.log(
            "PharmGKB - Starting the writing gene names to the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.addBiopolymerTypedNameNamespacedNames(
            typeID["gene"], namespaceID["pharmgkb_gid"], setNames
        )
        self.log(
            "PharmGKB - Writing gene names to the database completed",
            level=logging.INFO,
            indent=2,
        )
        setNames = None

        # process pathways
        self.log(
            "PharmGKB - Starting the verifying pathway archive file ...",
            level=logging.INFO,
            indent=2,
        )
        pathDesc = {}
        nsAssoc = {
            "pharmgkb_gid": set(),
            "symbol": set(),
        }
        numAssoc = numID = 0
        with zipfile.ZipFile(path + "/pathways-tsv.zip", "r") as pathZip:
            err = pathZip.testzip()
            if err:
                self.log(
                    "PharmGKB - CRC failed for %s\n" % err,
                    level=logging.ERROR,
                    indent=2,
                )
                raise Exception("ERROR: CRC failed for %s" % err)
                # return False

            self.log(
                "PharmGKB - Verifying pathway archive file completed",
                level=logging.INFO,
                indent=2,
            )
            self.log(
                "PharmGKB - Starting the processing pathways ...",
                level=logging.INFO,
                indent=2,
            )
            for info in pathZip.infolist():
                if info.filename == "pathways.tsv":
                    # the old format had all pathways in one giant file,
                    # delimited by blank lines
                    pathFile = pathZip.open(path + "/" + info, "r")
                    curPath = None
                    lastline = ""
                    for line in pathFile:
                        line = line.decode("latin-1").rstrip("\r\n")
                        if line == "" and lastline == "":
                            curPath = None
                        elif curPath is None:
                            words = line.split(":", 1)
                            if len(words) >= 2:
                                curPath = words[0].strip()
                                desc = words[1].strip().rsplit(" - ", 1)
                                desc.append("")
                                pathDesc[curPath] = (
                                    desc[0].strip().replace("`", "'"),
                                    desc[1].strip().replace("`", "'"),
                                )
                        elif curPath is False:
                            pass
                        else:
                            words = line.split("\t")
                            if words[0] == "From":
                                curPath = False
                            elif words[0] == "Gene":
                                pgkbID = words[1]
                                symbol = words[2]

                                numAssoc += 1
                                numID += 2
                                nsAssoc["pharmgkb_gid"].add(
                                    (curPath, numAssoc, pgkbID)
                                )  # noqa E501
                                nsAssoc["symbol"].add(
                                    (curPath, numAssoc, symbol)
                                )  # noqa E501
                            # if assoc is Gene
                        lastline = line
                    # foreach line in pathFile
                    pathFile.close()
                elif info.filename.endswith(".tsv"):
                    # the new format has separate "PA###-***.tsv" files for
                    # each pathway
                    pathFile = pathZip.open(info, "r")
                    header = next(pathFile)
                    if header.decode().startswith(
                        "From	To	Reaction Type	Controller	Control Type	Cell Type	PubMed Id	Genes"  # noqa E501
                    ):  # Drugs	Diseases
                        pass
                    elif header.decode().startswith(
                        "From	To	Reaction Type	Controller	Control Type	Cell Type	PMIDs	Genes"  # noqa E501
                    ):  # Drugs	Diseases
                        pass
                    else:
                        self.log(
                            "ERROR: unrecognized file header in '%s': %s"
                            % (info.filename, header),
                            level=logging.ERROR,
                            indent=2,
                        )
                        raise Exception(
                            "ERROR: unrecognized file header in '%s': %s"
                            % (info.filename, header)
                        )
                    parts = info.filename.split("-")
                    curPath = parts[0]
                    parts = parts[1].split(".")
                    pathDesc[curPath] = (
                        parts[0].replace("_", " "),
                        None,
                    )
                    for line in pathFile:
                        for symbol in (
                            line.decode("latin-1").split("\t")[7].split(",")
                        ):  # noqa E501
                            numAssoc += 1
                            numID += 1
                            nsAssoc["symbol"].add(
                                (curPath, numAssoc, symbol.strip('"'))
                            )
                    # foreach line in pathFile
                    pathFile.close()
                # if pathways.tsv
            # foreach file in pathZip
        # with pathZip
        self.log(
            "PharmGKB - Processing pathways completed: %d pathways, %d associations (%d identifiers)"  # noqa E501
            % (len(pathDesc), numAssoc, numID),
            level=logging.INFO,
            indent=2,
        )

        # store pathways
        self.log(
            "PharmGKB - Starting the writing pathways to the database ...",
            level=logging.INFO,
            indent=2,
        )
        listPath = pathDesc.keys()
        listGID = self.addTypedGroups(
            typeID["pathway"], (pathDesc[path] for path in listPath)
        )
        pathGID = dict(zip(listPath, listGID))
        self.log(
            "PharmGKB - Writing pathways to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # store pathway names
        self.log(
            "PharmGKB - Starting the writing pathway names to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        self.addGroupNamespacedNames(
            namespaceID["pharmgkb_id"],
            ((pathGID[path], path) for path in listPath),  # noqa E501
        )
        self.addGroupNamespacedNames(
            namespaceID["pathway"],
            ((pathGID[path], pathDesc[path][0]) for path in listPath),
        )
        self.log(
            "PharmGKB - Writing pathway names to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # store gene associations
        self.log(
            "PharmGKB - Starting the writing gene associations to the database ...",
            level=logging.INFO,
            indent=2,
        )
        for ns in nsAssoc:
            self.addGroupMemberTypedNamespacedNames(
                typeID["gene"],
                namespaceID[ns],
                ((pathGID[a[0]], a[1], a[2]) for a in nsAssoc[ns]),
            )
        self.log(
            "PharmGKB - Writing gene associations to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # Finalize the process
        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"PharmGKB - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"PharmGKB - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )

        # TODO: eventually add diseases, drugs, relationships
