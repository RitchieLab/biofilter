import os
import re
from loki_modules import loki_source


class Source_go(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "2.1 (2022-04-14)"

    # getVersionString()

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
        self.log("deleting old records from the database ...\n")
        self.deleteAll()
        self.log("deleting old records from the database completed\n")

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
        subtypeID = self.addSubtypes(
            [
                ("-",),
            ]
        )

        # process ontology terms
        self.log("processing ontology terms ...\n")
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
            "processing ontology terms completed: %d terms, %d links\n"
            % (numTerms, numLinks)
        )

        # store ontology terms
        self.log("writing ontology terms to the database ...\n")
        listGoID = goName.keys()
        listGID = self.addTypedGroups(
            typeID["ontology"],
            ((subtypeID["-"], goName[goID], goDef[goID]) for goID in listGoID),
        )
        goGID = dict(zip(listGoID, listGID))
        self.log("writing ontology terms to the database completed\n")

        # store ontology term names
        self.log("writing ontology term names to the database ...\n")
        self.addGroupNamespacedNames(
            namespaceID["go_id"], ((goGID[goID], goID) for goID in listGoID)
        )
        self.addGroupNamespacedNames(
            namespaceID["ontology"],
            ((goGID[goID], goName[goID]) for goID in listGoID),  # noqa E501
        )
        self.log("writing ontology term names to the database completed\n")

        # store ontology term links
        self.log("writing ontology term relationships to the database ...\n")
        listLinks = []
        for goID in goLinks:
            for link in goLinks[goID] or empty:
                if link[0] in goGID:
                    listLinks.append(
                        (goGID[goID], goGID[link[0]], link[1], link[2])
                    )  # noqa E501
        self.addGroupRelationships(listLinks)
        self.log(
            "writing ontology term relationships to the database completed\n"
        )  # noqa E501

        # process gene associations
        self.log("processing gene associations ...\n")
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
            "processing gene associations completed: %d associations (%d identifiers)\n"  # noqa E501
            % (numAssoc, numID)
        )

        # store gene associations
        self.log("writing gene associations to the database ...\n")
        for ns in nsAssoc:
            self.addGroupMemberTypedNamespacedNames(
                typeID["gene"], namespaceID[ns], nsAssoc[ns]
            )
        self.log("writing gene associations to the database completed\n")
