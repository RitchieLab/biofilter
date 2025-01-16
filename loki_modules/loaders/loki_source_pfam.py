import collections
from loki_modules import loki_source


class Source_pfam(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        self.downloadFilesFromHTTP(
            "ftp.ebi.ac.uk",
            {
                path
                + "/pfamA.txt.gz": "/pub/databases/Pfam/current_release/database_files/pfamA.txt.gz",  # noqa E501
                path
                + "/pfamA_reg_full_significant.txt.gz": "/pub/databases/Pfam/current_release/database_files/pfamA_reg_full_significant.txt.gz",  # noqa E501
                path
                + "/pfamseq.txt.gz": "/pub/databases/Pfam/current_release/database_files/pfamseq.txt.gz",  # noqa E501
            },
        )

        return [
            path + "/pfamA.txt.gz",
            path + "/pfamA_reg_full_significant.txt.gz",
            path + "/pfamseq.txt.gz",
        ]

    def update(self, options, path):
        # clear out all old data from this source
        self.log("deleting old records from the database ...\n")
        self.deleteAll()
        self.log("deleting old records from the database completed\n")

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [
                ("pfam_id", 0),
                ("proteinfamily", 0),
                ("uniprot_pid", 1),
            ]
        )
        relationshipID = self.addRelationships(
            [
                ("",),
            ]
        )
        typeID = self.addTypes(
            [
                ("proteinfamily",),
                ("gene",),
            ]
        )
        subtypeID = self.addSubtypes(
            [
                ("-",),
            ]
        )

        # process protein families
        self.log("processing protein families ...\n")
        # TODO:context manager,iterator
        pfamFile = self.zfile(path + "/pfamA.txt.gz")
        groupFam = collections.defaultdict(set)
        famAcc = {}
        famID = {}
        famName = {}
        famDesc = {}
        for line in pfamFile:
            words = line.split("\t", 10)
            pfamNum = words[0].strip()
            if pfamNum.isdigit():
                pfamNum = int(pfamNum)  # auto_pfamA = 1 , 2 , ...
                pfamAcc = words[1].strip()  # pfamA_acc = PF00389 , PF00198
                pfamID = words[
                    2
                ].strip()  # pfamA_id = 2-Hacid_dh , 2-oxoacid_dh  # noqa E501
                name = words[
                    4
                ].strip()  # description = D-isomer specific 2-hydroxyacid dehydrogenase, catalytic domain   # noqa E501
                group = words[
                    8
                ].strip()  # type = Domain , Family , Motif , Repeat  # noqa E501
                desc = words[9].strip()  # comment = (long description)
            else:
                # starting in release 28, all the "auto" columns were dropped
                pfamAcc = pfamNum
                pfamID = words[1].strip()  # 2-Hacid_dh , 2-oxoacid_dh , ...
                name = words[
                    3
                ].strip()  # D-isomer specific 2-hydroxyacid dehydrogenase, catalytic domain  # noqa E501
                group = words[7].strip()  # Domain , Family , Motif , Repeat
                desc = words[8].strip()  # (long description)

            groupFam[group].add(pfamNum)
            famAcc[pfamNum] = pfamAcc
            famID[pfamNum] = pfamID
            famName[pfamNum] = name
            famDesc[pfamNum] = desc
        numGroup = len(groupFam)
        numFam = len(famName)
        self.log(
            "processing protein families completed: %d categories, %d families\n"  # noqa E501
            % (numGroup, numFam)
        )

        # store protein families
        self.log("writing protein families to the database ...\n")
        listGroup = groupFam.keys()
        listGID = self.addTypedGroups(
            typeID["proteinfamily"],
            ((subtypeID["-"], group, "") for group in listGroup),
        )
        groupGID = dict(zip(listGroup, listGID))
        listFam = famAcc.keys()
        listGID = self.addTypedGroups(
            typeID["proteinfamily"],
            ((subtypeID["-"], famName[fam], famDesc[fam]) for fam in listFam),
        )
        famGID = dict(zip(listFam, listGID))
        self.log("writing protein families to the database completed\n")

        # store protein family names
        self.log("writing protein family names to the database ...\n")
        self.addGroupNamespacedNames(
            namespaceID["pfam_id"],
            ((groupGID[group], group) for group in listGroup),  # noqa E501
        )
        self.addGroupNamespacedNames(
            namespaceID["pfam_id"],
            ((famGID[fam], famAcc[fam]) for fam in listFam),  # noqa E501
        )
        self.addGroupNamespacedNames(
            namespaceID["proteinfamily"],
            ((famGID[fam], famID[fam]) for fam in listFam),  # noqa E501
        )
        self.addGroupNamespacedNames(
            namespaceID["proteinfamily"],
            ((famGID[fam], famName[fam]) for fam in listFam),
        )
        famName = famDesc = None
        self.log("writing protein family names to the database completed\n")

        # store protein family meta-group links
        self.log("writing protein family links to the database ...\n")
        for group in groupFam:
            self.addGroupRelationships(
                (famGID[fam], groupGID[group], relationshipID[""], None)
                for fam in groupFam[group]
            )
        groupFam = None
        self.log("writing protein family links to the database completed\n")

        # process protein identifiers
        self.log("processing protein identifiers ...\n")
        # TODO:context manager,iterator
        seqFile = self.zfile(path + "/pfamseq.txt.gz")
        proNames = dict()
        for line in seqFile:
            words = line.split("\t", 10)
            proteinNum = words[0].strip()
            if proteinNum.isdigit():
                proteinNum = int(proteinNum)  # auto_pfamseq = 1 , 2
                uniprotID = words[1]  # pfamseq_id = 1433B_HUMAN ,GATC_HUMAN
                uniprotAcc = words[2]  # pfamseq_acc = P31946 , O43716
                species = words[9]  # species = Homo sapiens (Human)
            else:
                # starting in release 28, all the "auto" columns were dropped
                uniprotID = proteinNum  # pfamseq_id = 1433B_HUMAN ,GATC_HUMAN
                uniprotAcc = words[1]  # pfamseq_acc = P31946 , O43716
                species = words[8]  # species = Homo sapiens (Human)

            if species == "Homo sapiens (Human)":
                proNames[proteinNum] = (uniprotID, uniprotAcc)
        # foreach protein
        self.log(
            "processing protein identifiers completed: %d proteins\n"
            % (len(proNames),)  # noqa E501
        )

        # process associations
        self.log("processing protein associations ...\n")
        # TODO:context manager,iterator
        assocFile = self.zfile(path + "/pfamA_reg_full_significant.txt.gz")
        setAssoc = set()
        numAssoc = numID = 0
        for line in assocFile:
            words = line.split("\t", 15)
            pfamNum = words[1].strip()
            if pfamNum.isdigit():
                pfamNum = int(pfamNum)  # auto_pfamA
                proteinNum = int(words[2])  # auto_pfamseq
                inFull = int(words[14])  # in_full
            else:
                # starting in release 28, all the "auto" columns were dropped
                pfamNum = pfamNum  # pfamA_acc
                proteinNum = words[2].strip()  # pfamseq_acc
                inFull = int(words[14])  # in_full

            if (pfamNum in famGID) and (proteinNum in proNames) and inFull:
                numAssoc += 1
                numID += len(proNames[proteinNum])
                for name in proNames[proteinNum]:
                    setAssoc.add((famGID[pfamNum], numAssoc, name))
            # if association is ok
        # foreach association
        self.log(
            "processing protein associations completed: %d associations (%d identifiers)\n"  # noqa E501
            % (numAssoc, numID)
        )

        # store gene associations
        self.log("writing gene associations to the database ...\n")
        self.addGroupMemberTypedNamespacedNames(
            typeID["gene"], namespaceID["uniprot_pid"], setAssoc
        )
        self.log("writing gene associations to the database completed\n")
