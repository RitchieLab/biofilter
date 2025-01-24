import os
import re
import logging
import psutil
import time
from loki_modules import loki_source


class Source_go(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        # download the latest source files
        self.downloadFilesFromHTTP(
            "current.geneontology.org",
            {
                path + "/goa_human.gaf.gz": "/annotations/goa_human.gaf.gz",
                path + "/go.obo": "/ontology/go.obo",
            },
        )

        return [path + "/goa_human.gaf.gz", path + "/go.obo"]

    def update(self, options, path):
        # clear out all old data from this source
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"GO - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "GO - Starting deletion of old records from the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "GO - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [
                ("go_id", 0),
                ("ontology", 0),
                ("symbol", 0),
                ("uniprot_pid", 1),
            ]
        )
        relationshipID = self.addRelationships(
            [
                ("is_a",),
            ]
        )
        typeID = self.addTypes(
            [
                ("ontology",),
                ("gene",),
            ]
        )

        # process ontology terms
        self.log(
            "GO - Starting the processing of ontology terms ...",
            level=logging.INFO,
            indent=2,
        )  # noqa E501
        # file format: http://www.geneontology.org/GO.format.obo-1_2.shtml
        # correctly handling all the possible escape seq and special cases
        # in the OBO spec would be somewhat involved, but the previous version
        # of biofilter used a much simpler approach which seemed to work ok in
        # practice, so we'll stick with that for now
        reTrailingEscape = re.compile("(?:^|[^\\\\])(?:\\\\\\\\)*\\\\$")
        empty = tuple()
        goName = {}
        goDef = {}
        goLinks = {}
        # goNS = {}
        # oboProps = {}
        curStanza = curID = curAnon = curObs = curName = curDef = curLinks = (
            None  # noqa E501
        )
        with open(path + "/go.obo", "r") as oboFile:
            while True:
                try:
                    line = next(oboFile).rstrip()
                    parts = line.split("!", 1)[0].split(":", 1)
                    tag = parts[0].strip()
                    val = parts[1].strip() if (len(parts) > 1) else None
                except StopIteration:
                    line = False

                if line is False or tag.startswith("["):
                    if (
                        (curStanza == "Term")
                        and curID
                        and (not curAnon)
                        and (not curObs)
                    ):
                        goName[curID] = curName
                        goDef[curID] = curDef
                        goLinks[curID] = curLinks or empty
                    if line is False:
                        break
                    curStanza = tag[1 : tag.index("]")]  # noqa E203
                    curID = curAnon = curObs = curName = curDef = curLinks = (
                        None  # noqa E501
                    )
                elif tag == "id":
                    curID = val
                elif tag == "alt_id":
                    pass
                elif tag == "def":
                    curDef = val
                    if val.startswith('"'):
                        curDef = ""
                        words = val.split('"')
                        for w in range(1, len(words)):
                            curDef += words[w]
                            if not reTrailingEscape.search(words[w]):
                                break
                elif tag == "is_anonymous":
                    curAnon = val.lower().split()[0] == "true"
                elif tag == "is_obsolete":
                    curObs = val.lower().split()[0] == "true"
                elif tag == "replaced_by":
                    pass
                # elif tag == 'namespace':
                # 	curNS = val
                elif tag == "name":
                    curName = val
                elif tag == "synonym":
                    pass
                elif tag == "xref":
                    pass
                elif tag == "is_a":
                    curLinks = curLinks or set()
                    curLinks.add((val.split()[0], relationshipID["is_a"], -1))
                elif tag == "relationship":
                    curLinks = curLinks or set()
                    words = val.split()
                    if words[0] not in relationshipID:
                        relationshipID[words[0]] = self.addRelationship(
                            words[0]
                        )  # noqa E501
                    if words[0] == "part_of":
                        contains = -1
                    elif words[0] in (
                        "regulates",
                        "positively_regulates",
                        "negatively_regulates",
                    ):
                        contains = 0
                    else:
                        contains = None
                    curLinks.add(
                        (words[1], relationshipID[words[0]], contains)
                    )  # noqa E501
            # foreach line
        # with oboFile
        numTerms = len(goName)
        numLinks = sum(len(goLinks[goID]) for goID in goLinks)
        self.log(
            "GO - Processing ontology terms completed: %d terms, %d links"
            % (numTerms, numLinks),
            level=logging.INFO,
            indent=2,
        )

        # store ontology terms
        self.log(
            "GO - Starting the writing ontology terms to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        listGoID = goName.keys()
        listGID = self.addTypedGroups(
            typeID["ontology"],
            ((goName[goID], goDef[goID]) for goID in listGoID),
        )
        goGID = dict(zip(listGoID, listGID))
        self.log(
            "GO - Writing ontology terms to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # store ontology term names
        self.log(
            "GO - Starting the writing ontology terms names to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        self.addGroupNamespacedNames(
            namespaceID["go_id"], ((goGID[goID], goID) for goID in listGoID)
        )
        self.addGroupNamespacedNames(
            namespaceID["ontology"],
            ((goGID[goID], goName[goID]) for goID in listGoID),  # noqa E501
        )
        self.log(
            "GO - Writing ontology terms names to the database completed",
            level=logging.INFO,
            indent=2,
        )

        # store ontology term links
        self.log(
            "GO - Starting the writing ontology terms relationships to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        listLinks = []
        for goID in goLinks:
            for link in goLinks[goID] or empty:
                if link[0] in goGID:
                    listLinks.append(
                        (goGID[goID], goGID[link[0]], link[1], link[2])
                    )  # noqa E501
        self.addGroupRelationships(listLinks)
        self.log(
            "GO - Writing ontology terms relationships to the database completed",  # noqa E501
            level=logging.INFO,
            indent=2,
        )

        # process gene associations
        self.log(
            "GO - Starting the processing gene associations ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        if os.path.isfile(
            path + "/gene_association.goa_human.gz"
        ) and not os.path.isfile(path + "/goa_human.gaf.gz"):
            assocFile = self.zfile(
                path + "/gene_association.goa_human.gz"
            )  # TODO:context manager,iterator
        else:
            assocFile = self.zfile(
                path + "/goa_human.gaf.gz"
            )  # TODO:context manager,iterator
        nsAssoc = {"uniprot_pid": set(), "symbol": set()}
        numAssoc = numID = 0
        for line in assocFile:
            words = line.split("\t")
            if len(words) < 13:
                continue
            xrefDB = words[0]
            xrefID = words[1]
            gene = words[2]
            # assocType = words[3]
            goID = words[4]
            # reference = words[5]
            evidence = words[6]
            # withID = words[7]
            # goType = words[8]
            # desc = words[9]
            aliases = words[10].split("|")
            # xrefType = words[11]
            taxon = words[12]
            # updated = words[13]
            # assigner = words[14]
            # extensions = words[15].split('|')
            # xrefIDsplice = words[16]

            # TODO: find out for sure why the old Biofilter loader ignores IEA
            if (
                xrefDB == "UniProtKB"
                and goID in goGID
                and evidence != "IEA"
                and taxon == "taxon:9606"
            ):
                numAssoc += 1
                numID += 2
                nsAssoc["uniprot_pid"].add((goGID[goID], numAssoc, xrefID))
                nsAssoc["symbol"].add((goGID[goID], numAssoc, gene))
                for alias in aliases:
                    numID += 1
                    # aliases might be either symbols or uniprot identifiers,
                    # so try them both ways
                    nsAssoc["uniprot_pid"].add((goGID[goID], numAssoc, alias))
                    nsAssoc["symbol"].add((goGID[goID], numAssoc, alias))
            # if association is ok
        # foreach association
        self.log(
            "GO - Processing gene associations completed: %d associations (%d identifiers)"  # noqa E501
            % (numAssoc, numID),
            level=logging.INFO,
            indent=2,
        )

        # store gene associations
        self.log(
            "GO - Starting the writing gene associations to the database ...",  # noqa E501
            level=logging.INFO,
            indent=2,
        )
        for ns in nsAssoc:
            self.addGroupMemberTypedNamespacedNames(
                typeID["gene"], namespaceID[ns], nsAssoc[ns]
            )
        self.log(
            "GO - Writing gene associations to the database completed",  # noqa E501
            level=logging.INFO,
            indent=2,
        )

        # Finalize the process
        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"GO - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"GO - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )
