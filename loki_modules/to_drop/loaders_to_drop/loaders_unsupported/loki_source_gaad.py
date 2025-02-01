#!/usr/bin/env python

import collections
import re
from loki import loki_source


class Source_gaad(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "1.0 (2023-06-08)"

    # getVersionString()

    def download(self, options):
        # download the latest source files
        self.downloadFilesFromHTTPS(
            "gaad.medgenius.info",
            {
                "diseases2.txt.gz": "/Downloads/diseases2.txt.gz",  # disease name by AID
                "disease_relationships.txt.gz": "/Downloads/disease_relationships.txt.gz",
                "disease_association_database_annotations_uniprot_ncbiGene.txt.gz": "/Downloads/disease_association_database_annotations_uniprot_ncbiGene.txt.gz",
                "disease_association_genecards.txt.gz": "/Downloads/disease_association_genecards.txt.gz",
                "disease_gene_association_pubmed_textmining_zhao.txt.gz": "/Downloads/disease_gene_association_pubmed_textmining_zhao.txt.gz",
            },
        )

    # download()

    def update(self, options):
        # clear out all old data from this source
        self.log("deleting old records from the database ...")
        self.deleteAll()
        self.log(" OK\n")

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [("gaad_id", 0), ("entrez_gid", 0), ("disease", 0)]
        )
        relationshipID = self.addRelationships(
            [
                ("disease_co-occurring",),
            ]
        )
        typeID = self.addTypes(
            [
                ("disease",),
                ("gene",),
            ]
        )
        subtypeID = self.addSubtypes(
            [
                ("-",),
            ]
        )

        # process gaad disease
        self.log("processing diseases ...")
        diseaseFile = self.zfile("diseases2.txt.gz")
        diseases = {}
        for line in diseaseFile:
            if not line.startswith("AID"):
                continue
            words = line.split("\t")
            diseaseID = words[0]
            name = words[1].rstrip()
            # store disease name of each disease ID (AID)
            diseases[diseaseID] = name
        # foreach line in diseaseFile
        self.log(" OK: %d disease\n" % (len(diseases),))

        # store diseases
        self.log("writing diseases to the database ...")
        listGroup = diseases.keys()
        listAID = self.addTypedGroups(
            typeID["disease"],
            ((subtypeID["-"], group, diseases[group]) for group in listGroup),
        )
        groupAID = dict(zip(listGroup, listAID))
        self.log(" OK\n")

        # store diseases names
        self.log("writing diseases names to the database ...")
        self.addGroupNamespacedNames(
            namespaceID["gaad_id"], ((groupAID[group], group) for group in listGroup)
        )
        self.addGroupNamespacedNames(
            namespaceID["disease"],
            ((groupAID[group], diseases[group]) for group in listGroup),
        )
        diseases = None
        self.log(" OK\n")

        # process gaad disease relationships
        self.log("processing diseases relationships ...")
        relationshipFile = self.zfile("disease_relationships.txt.gz")
        relationships = []
        num = 0
        for line in relationshipFile:
            if line.startswith("disease_uid1"):
                continue
            words = line.split("\t")
            diseaseID = words[0]
            diseaseID2 = words[1]
            # store disease pairs that shares genes
            relationships.append(
                (diseaseID, diseaseID2, relationshipID["disease_co-occurring"], None)
            )
            num += 1
        # foreach line in diseaseFile
        self.log(" OK: %d disease relationships\n" % (num,))

        # store gaad disease relationships
        self.log("writing diseases relationships to the database ...")
        self.addGroupRelationships(relationships)
        relationships = None
        self.log(" OK\n")

        # process gaad disease identifiers
        self.log("processing diseases identifiers ...")
        ncbiFile = self.zfile(
            "disease_association_database_annotations_uniprot_ncbiGene.txt.gz"
        )
        genecardsFile = self.zfile("disease_association_genecards.txt.gz")
        pubmedFile = self.zfile(
            "disease_gene_association_pubmed_textmining_zhao.txt.gz"
        )
        diseaseGene = []
        num = 0
        for line in ncbiFile:
            if line.startswith("disease_"):
                continue
            words = line.split("\t")
            diseaseID = words[0].strip()
            entrezID = words[1].strip()
            num += 1
            diseaseGene.append((groupAID[diseaseID], num, entrezID))
        # foreach line in ncbiFile:
        for line in genecardsFile:
            if line.startswith("disease_"):
                continue
            words = line.split("\t")
            diseaseID = words[0].strip()
            entrezID = words[1].strip()
            num += 1
            diseaseGene.append((groupAID[diseaseID], num, entrezID))
        # foreach line in genecardsFile:
        for line in pubmedFile:
            if line.startswith("disease_"):
                continue
            words = line.split("\t")
            diseaseID = words[2].strip()
            entrezID = words[1].strip()
            num += 1
            diseaseGene.append((groupAID[diseaseID], num, entrezID))
        # foreach line in pubmedFile:
        self.log(" OK: %d diseases and gene pairs\n" % (len(diseaseGene),))

        # store gaad disease identifiers
        self.log("writing diseases and gene pairs to the database ...")
        self.addGroupMemberTypedNamespacedNames(
            typeID["gene"], namespaceID["entrez_gid"], diseaseGene
        )
        diseaseGene = None
        self.log(" OK\n")

    # update()


# Source_go
