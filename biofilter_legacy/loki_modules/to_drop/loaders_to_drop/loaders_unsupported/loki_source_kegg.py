#!/usr/bin/env pytihon

import json

from loki import loki_source


class Source_kegg(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "2.0 (2013-02-14)"

    # getVersionString()

    @classmethod
    def getOptions(cls):
        return {
            "api": "[rest|cache]  --  use the new REST API, or a local file cache (default: rest)"
        }

    # getOptions()

    def validateOptions(self, options):
        for o, v in options.items():
            if o == "api":
                v = v.strip().lower()
                if "rest".startswith(v):
                    v = "rest"
                elif "cache".startswith(v):
                    v = "cache"
                else:
                    return "api must be 'rest', or 'cache'"
                options[o] = v
            else:
                return "unexpected option '%s'" % o
        return True

    # validateOptions()

    def download(self, options):
        if options.get("api") == "cache":
            # do nothing, update() will just expect the files to already be there
            pass
        else:  # api==rest
            self.downloadFilesFromHTTP(
                "rest.kegg.jp",
                {
                    "list-pathway-hsa": "/list/pathway/hsa",
                    "link-pathway-hsa": "/link/pathway/hsa",
                    "list-disease": "/list/disease",
                    "link-disease-hsa": "/link/disease/hsa",
                    "category-pathway": "/get/br:br08901/json",
                    "category-disease": "/get/br:br08403/json",
                },
            )
        # if api==rest/cache

    # download()

    def update(self, options):
        # clear out all old data from this source
        self.log("deleting old records from the database ...")
        self.deleteAll()
        self.log(" OK\n")

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [("kegg_id", 0), ("pathway", 0), ("entrez_gid", 0), ("disease", 0)]
        )
        typeID = self.addTypes(
            [
                ("pathway",),
                ("gene",),
                ("disease",),
            ]
        )

        # process pathways
        self.log("processing pathways ...")
        # read pathway categories json file into pathCategory
        pathCategory = []
        with open(r"category-pathway") as pathCategoryFile:
            pathCategory = json.load(pathCategoryFile)
        # store subtypes into pathSubtype
        pathSubtype = {}
        for category in pathCategory["children"]:
            for category2 in category["children"]:
                if (
                    category2["name"] == "Global and overview maps"
                    or category2["name"] == "Carbohydrate metabolism"
                    or category2["name"] == "Energy metabolism"
                    or category2["name"] == "Immune system"
                    or category2["name"] == "Endocrine system"
                ):
                    continue
                for category3 in category2["children"]:
                    line = category3["name"].split("  ")
                    pathID = "hsa" + line[0]
                    pathSubtype[pathID] = category2["name"]
        pathCategory = None
        # with pathCategory
        pathName = {}
        with open("list-pathway-hsa", "rU") as pathFile:
            for line in pathFile:
                words = line.split("\t")
                pathID = words[0]
                if pathID not in pathSubtype:
                    pathSubtype[pathID] = "-"
                name = words[1].rstrip()
                if name.endswith(" - Homo sapiens (human)"):
                    name = name[:-23]
                pathName[pathID] = name
            # foreach line in pathFile
        # with pathFile
        self.log(" OK: %d pathways\n" % (len(pathName),))

        # store pathways
        self.log("writing pathways to the database ...")
        listPath = pathName.keys()
        listSubtype = self.addSubtypes([(val,) for val in set(pathSubtype.values())])
        listGID = self.addTypedGroups(
            typeID["pathway"],
            (
                (listSubtype[pathSubtype[pathID]], pathName[pathID], None)
                for pathID in listPath
            ),
        )
        pathGID = dict(zip(listPath, listGID))
        self.log(" OK\n")

        # store pathway names
        self.log("writing pathway names to the database ...")
        self.addGroupNamespacedNames(
            namespaceID["kegg_id"], ((pathGID[pathID], pathID) for pathID in listPath)
        )
        self.addGroupNamespacedNames(
            namespaceID["pathway"],
            ((pathGID[pathID], pathName[pathID]) for pathID in listPath),
        )
        self.log(" OK\n")
        pathName = None
        listPath = None

        # process associations
        self.log("processing pathway gene associations ...")
        entrezAssoc = set()
        numAssoc = 0
        with open("link-pathway-hsa", "rU") as assocFile:
            for line in assocFile:
                words = line.split("\t")
                hsaGene = words[0]
                pathID = words[1].strip().replace("path:hsa", "hsa")
                if pathID in pathGID:
                    numAssoc += 1
                    entrezAssoc.add((pathGID[pathID], numAssoc, hsaGene[4:]))
                # if pathway and gene are ok
            # foreach line in assocFile
        # with assocFile
        self.log(" OK: %d associations\n" % (numAssoc,))
        listSubtype = None
        pathGID = None

        # store gene associations
        self.log("writing gene associations to the database ...")
        self.addGroupMemberTypedNamespacedNames(
            typeID["gene"], namespaceID["entrez_gid"], entrezAssoc
        )
        self.log(" OK\n")
        entrezAssoc = None

        # process diseases
        self.log("processing diseases ...")
        # read disease categories json file into diseaseCategory
        diseaseCategory = []
        with open(r"category-disease") as diseaseCategoryFile:
            diseaseCategory = json.load(diseaseCategoryFile)
        # store subtypes into diseaseSubtype
        diseaseSubtype = {}
        for category in diseaseCategory["children"]:
            for category2 in category["children"]:
                if "children" not in category2:
                    continue
                for category3 in category2["children"]:
                    if "children" not in category3:
                        continue
                    for category4 in category3["children"]:
                        line = category4["name"]
                        if not line.startswith("H"):
                            continue
                        diseaseID = line.split("  ")[0]
                        diseaseSubtype[diseaseID] = category2["name"]
        diseaseCategory = None
        # with diseaseCategory
        diseaseName = {}
        with open("list-disease", "rU") as pathFile:
            for line in pathFile:
                words = line.split("\t")
                pathID = words[0]
                if pathID not in diseaseSubtype:
                    diseaseSubtype[pathID] = "-"
                name = words[1].rstrip()
                diseaseName[pathID] = name
            # foreach line in diseaseFile
        # with diseaseFile
        self.log(" OK: %d diseases\n" % (len(diseaseName),))

        # store diseases
        self.log("writing diseases to the database ...")
        listDisease = diseaseName.keys()
        listSubtype = self.addSubtypes([(val,) for val in set(diseaseSubtype.values())])
        listGID = self.addTypedGroups(
            typeID["disease"],
            (
                (listSubtype[diseaseSubtype[diseaseID]], diseaseName[diseaseID], None)
                for diseaseID in listDisease
            ),
        )
        diseaseGID = dict(zip(listDisease, listGID))
        self.log(" OK\n")

        # store disease names
        self.log("writing disease names to the database ...")
        self.addGroupNamespacedNames(
            namespaceID["kegg_id"],
            ((diseaseGID[diseaseID], diseaseID) for diseaseID in listDisease),
        )
        self.addGroupNamespacedNames(
            namespaceID["disease"],
            (
                (diseaseGID[diseaseID], diseaseName[diseaseID])
                for diseaseID in listDisease
            ),
        )
        self.log(" OK\n")

        # process disease & gene associations
        self.log("processing disease gene associations ...")
        entrezAssoc = set()
        numAssoc = 0
        with open("link-disease-hsa", "rU") as assocFile:
            for line in assocFile:
                words = line.split("\t")
                hsaGene = words[0]
                diseaseID = words[1].strip()[3:]
                if diseaseID in diseaseGID:
                    numAssoc += 1
                    entrezAssoc.add((diseaseGID[diseaseID], numAssoc, hsaGene[4:]))
            # foreach line in assocFile
        # with assocFile
        self.log(" OK: %d associations\n" % (numAssoc,))

        # store gene associations
        self.log("writing gene associations to the database ...")
        self.addGroupMemberTypedNamespacedNames(
            typeID["gene"], namespaceID["entrez_gid"], entrezAssoc
        )
        self.log(" OK\n")
        entrezAssoc = None

    # update()


# Source_kegg
