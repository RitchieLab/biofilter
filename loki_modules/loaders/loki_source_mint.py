import datetime
import os
import re
from loki_modules import loki_source


class Source_mint(loki_source.Source):

    ##################################################
    # private class methods

    def _identifyLatestFilename(self, filenames):
        reFile = re.compile(
            "^([0-9]+)-([0-9]+)-([0-9]+)-mint-human.txt$", re.IGNORECASE
        )
        bestdate = datetime.date.min
        bestfile = None
        for filename in filenames:
            match = reFile.match(filename)
            if match:
                filedate = datetime.date(
                    int(match.group(1)),
                    int(match.group(2)),
                    int(match.group(3)),  # noqa E501
                )
                if filedate > bestdate:
                    bestdate = filedate
                    bestfile = filename
        # foreach filename
        return bestfile

    ##################################################
    # source interface

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        self.downloadFilesFromHTTP(
            "www.ebi.ac.uk",
            {
                path
                + "/MINT_MiTab.txt": "/Tools/webservices/psicquic/mint/webservices/current/search/query/species:human",  # noqa E501
            },
        )

        return [path + "/MINT_MiTab.txt"]

    def update(self, options, path):
        # clear out all old data from this source
        self.log("deleting old records from the database ...\n")
        self.deleteAll()
        self.log("deleting old records from the database completed\n")

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [
                ("mint_id", 0),
                ("symbol", 0),
                ("entrez_gid", 0),
                ("ensembl_gid", 0),
                ("ensembl_pid", 1),
                ("refseq_gid", 0),
                ("refseq_pid", 1),
                ("uniprot_pid", 1),
            ]
        )
        typeID = self.addTypes(
            [
                ("interaction",),
                ("gene",),
            ]
        )
        subtypeID = self.addSubtypes(
            [
                ("-",),
            ]
        )

        # process interation groups
        self.log("processing interaction groups ...\n")
        mintDesc = dict()
        nsAssoc = {
            "symbol": set(),
            "entrez_gid": set(),
            "ensembl_gid": set(),
            "ensembl_pid": set(),
            "refseq_gid": set(),
            "refseq_pid": set(),
            "uniprot_pid": set(),
        }
        numAssoc = numID = 0
        if os.path.exists(path + "/MINT_MiTab.txt"):
            with open(path + "/MINT_MiTab.txt", "r") as assocFile:
                lx = 0
                for line in assocFile:
                    lx += 1
                    words = line.split("\t")

                    # skip non-human records
                    if not (
                        words[9].startswith("taxid:9606(")
                        and words[10].startswith("taxid:9606(")
                    ):
                        continue

                    # extract relevant columns
                    geneA = [
                        w.strip() for w in words[0].split("|") if w != "-"
                    ]  # id A  # noqa E501
                    geneB = [
                        w.strip() for w in words[1].split("|") if w != "-"
                    ]  # id B  # noqa E501
                    geneA.extend(
                        w.strip() for w in words[2].split("|") if w != "-"
                    )  # alt id A
                    geneB.extend(
                        w.strip() for w in words[3].split("|") if w != "-"
                    )  # alt id B
                    geneA.extend(
                        w.strip() for w in words[4].split("|") if w != "-"
                    )  # alias A
                    geneB.extend(
                        w.strip() for w in words[5].split("|") if w != "-"
                    )  # alias B
                    labels = dict(
                        (
                            w.strip().split(":", 1)
                            for w in words[13].split("|")
                            if w != "-"
                        )
                    )
                    if len(words) > 23:
                        geneA.extend(
                            w.strip() for w in words[22].split("|") if w != "-"
                        )  # xref A
                        geneB.extend(
                            w.strip() for w in words[23].split("|") if w != "-"
                        )  # xref B

                    # choose the group identifier
                    mintID = (
                        labels.get("mint")
                        or labels.get("intact")
                        or ("MINT-unlabeled-%d" % (lx,))
                    )
                    mintDesc[mintID] = ""

                    for names in (geneA, geneB):
                        numAssoc += 1
                        for name in names:
                            if ":" not in name:
                                continue
                            numID += 1
                            prefix, name = name.split(":", 1)
                            suffix = ""
                            if name.endswith(")"):
                                name, suffix = name[:-1].rsplit("(", 1)
                            if name.startswith('"'):
                                name = name.split('"')[1]

                            if prefix == "entrezgene/locuslink":
                                nsAssoc["entrez_gid"].add(
                                    (mintID, numAssoc, name)
                                )  # noqa E501
                            elif prefix == "ensembl":
                                namespace = (
                                    "ensembl_pid"
                                    if name.startswith("ENSP")
                                    else "ensembl_gid"
                                )
                                nsAssoc[namespace].add(
                                    (mintID, numAssoc, name)
                                )  # noqa E501
                            elif prefix == "refseq":
                                name = name.rsplit(".", 1)[0]
                                name = name.rsplit(",", 1)[0]
                                nsAssoc["refseq_gid"].add(
                                    (mintID, numAssoc, name)
                                )  # noqa E501
                                nsAssoc["refseq_pid"].add(
                                    (mintID, numAssoc, name)
                                )  # noqa E501
                            elif prefix == "uniprotkb":
                                if (suffix == "(gene name)") or (
                                    suffix == "(gene name synonym)"
                                ):
                                    namespace = "symbol"
                                else:
                                    namespace = "uniprot_pid"
                                    name = name.rsplit("-", 1)[0]
                                nsAssoc[namespace].add(
                                    (mintID, numAssoc, name)
                                )  # noqa E501
                            else:
                                numID -= 1
                            # if prefix/suffix
                        # foreach name
                    # foreach interactor
                # foreach line in assocFile
            # with assocFile
        else:  # old FTP file
            with open(
                self._identifyLatestFilename(os.listdir(path)), "r"
            ) as assocFile:  # noqa E501
                header = assocFile.next().rstrip()
                if not header.startswith(
                    "ID interactors A (baits)\tID interactors B (preys)\tAlt. ID interactors A (baits)\tAlt. ID interactors B (preys)\tAlias(es) interactors A (baits)\tAlias(es) interactors B (preys)\tInteraction detection method(s)\tPublication 1st author(s)\tPublication Identifier(s)\tTaxid interactors A (baits)\tTaxid interactors B (preys)\tInteraction type(s)\tSource database(s)\tInteraction identifier(s)\t"  # noqa E501
                ):  # Confidence value(s)\texpansion\tbiological roles A (baits)\tbiological role B\texperimental roles A (baits)\texperimental roles B (preys)\tinteractor types A (baits)\tinteractor types B (preys)\txrefs A (baits)\txrefs B (preys)\txrefs Interaction\tAnnotations A (baits)\tAnnotations B (preys)\tInteraction Annotations\tHost organism taxid\tparameters Interaction\tdataset\tCaution Interaction\tbinding sites A (baits)\tbinding sites B (preys)\tptms A (baits)\tptms B (preys)\tmutations A (baits)\tmutations B (preys)\tnegative\tinference\tcuration depth":  # noqa E501
                    self.log(" ERROR\n")
                    self.log("unrecognized file header: %s\n" % header)
                    return False
                xrefNS = {
                    "entrezgene/locuslink": ("entrez_gid",),
                    "refseq": ("refseq_gid", "refseq_pid"),
                    "uniprotkb": ("uniprot_pid",),
                }
                lx = 0
                for line in assocFile:
                    lx += 1
                    words = line.split("\t")
                    genes = words[0].split(";")
                    genes.extend(words[1].split(";"))
                    aliases = words[4].split(";")
                    aliases.extend(words[5].split(";"))
                    method = words[6]
                    taxes = words[9].split(";")
                    taxes.extend(words[10].split(";"))
                    labels = words[13].split("|")

                    # identify interaction group label
                    mint = None
                    for label in labels:
                        if label.startswith("mint:"):
                            mint = label
                            break
                    mint = mint or "MINT-unlabeled-%d" % lx
                    mintDesc[mint] = method

                    # identify interacting genes/proteins
                    for n in range(0, len(taxes)):
                        if taxes[n] == "taxid:9606(Homo sapiens)":
                            numAssoc += 1
                            # the "gene" is a helpful database cross-reference
                            # with a label indicating its type
                            xrefDB, xrefID = genes[n].split(":", 1)
                            if xrefDB in xrefNS:
                                numID += 1
                                if xrefDB == "refseq":
                                    xrefID = xrefID.rsplit(".", 1)[0]
                                elif xrefDB == "uniprotkb":
                                    xrefID = xrefID.rsplit("-", 1)[0]
                                for ns in xrefNS[xrefDB]:
                                    nsAssoc[ns].add((mint, numAssoc, xrefID))
                            # but the "alias" could be of any type and isn't
                            # identified, so we'll store copies under each
                            # possible type and find out later which one
                            # matches something
                            numID += 1
                            nsAssoc["symbol"].add((mint, numAssoc, aliases[n]))
                            nsAssoc["refseq_gid"].add(
                                (mint, numAssoc, aliases[n].rsplit(".", 1)[0])
                            )
                            nsAssoc["refseq_pid"].add(
                                (mint, numAssoc, aliases[n].rsplit(".", 1)[0])
                            )
                            nsAssoc["uniprot_pid"].add(
                                (mint, numAssoc, aliases[n].rsplit("-", 1)[0])
                            )
                        # if human
                    # foreach interacting gene/protein
                # foreach line in assocFile
            # with assocFile
        # if new/old file
        self.log(
            "processing interaction groups completed: %d groups, %d associations (%d identifiers)\n"  # noqa E501
            % (len(mintDesc), numAssoc, numID)
        )

        # store interaction groups
        self.log("writing interaction groups to the database ...\n")
        listMint = mintDesc.keys()
        listGID = self.addTypedGroups(
            typeID["interaction"],
            ((subtypeID["-"], mint, mintDesc[mint]) for mint in listMint),
        )
        mintGID = dict(zip(listMint, listGID))
        self.log("writing interaction groups to the database completed\n")

        # store interaction group names
        self.log("writing interaction group names to the database ...\n")
        self.addGroupNamespacedNames(
            namespaceID["mint_id"],
            ((mintGID[mint], mint) for mint in listMint),  # noqa E501
        )
        self.log("writing interaction group names to the database completed\n")

        # store gene interactions
        self.log("writing gene interactions to the database ...\n")
        for ns in nsAssoc:
            self.addGroupMemberTypedNamespacedNames(
                typeID["gene"],
                namespaceID[ns],
                ((mintGID[a[0]], a[1], a[2]) for a in nsAssoc[ns]),
            )
        self.log("writing gene interactions to the database completed\n")
