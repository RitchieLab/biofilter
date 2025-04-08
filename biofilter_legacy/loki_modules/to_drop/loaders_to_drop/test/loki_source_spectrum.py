#!/usr/bin/env python

from loki_modules import loki_source


class Source_spectrum(loki_source.Source):

    @classmethod
    def getVersionString(cls):
        return "3.0 (2023-02-22)"

    # getVersionString()

    def download(self, options):
        pass

    # download()

    def update(self, options):
        # clear out all old data from this source
        self.log("deleting old records from the database ...")
        self.deleteAll()
        self.log(" OK\n")

        # get or create the required metadata records
        namespaceID = self.addNamespaces(
            [
                ("gene", 0),
                ("protein", 1),
                ("group", 0),
            ]
        )
        typeID = self.addTypes(
            [
                ("gene",),
                ("group",),
            ]
        )
        subtypeID = self.addSubtypes(
            [
                ("-",),
            ]
        )

        # define groups
        self.log("adding groups to the database ...")
        listGroup = [
            # (label,description)
            (subtypeID["-"], "orange", "one protein, no ambiguity"),
            (subtypeID["-"], "indigo", "redundant proteins, extraneous gene"),
            (subtypeID["-"], "violet", "reducible protein ambiguity"),
        ]
        listGID = self.addTypedGroups(typeID["group"], listGroup)
        groupGID = dict(zip((g[1] for g in listGroup), listGID))
        self.log(" OK: %d groups\n" % len(groupGID))

        # define group names
        self.log("adding group names to the database ...")
        listName = [
            # (group_id,name)
            (groupGID["orange"], "orange"),
            (groupGID["indigo"], "indigo"),
            (groupGID["violet"], "violet"),
            (groupGID["violet"], "purple"),
        ]
        self.addGroupNamespacedNames(namespaceID["group"], listName)
        self.log(" OK: %d names\n" % len(listName))

        # define group members
        self.log("adding group members to the database ...")
        listMember = [
            # (group_id,member,type_id,namespace_id,name)
            (groupGID["orange"], 11, typeID["gene"], namespaceID["gene"], "P"),
            (groupGID["orange"], 11, typeID["gene"], namespaceID["gene"], "Q"),
            (groupGID["orange"], 11, typeID["gene"], namespaceID["gene"], "R"),
            (groupGID["orange"], 11, typeID["gene"], namespaceID["protein"], "pqr"),
            (groupGID["indigo"], 21, typeID["gene"], namespaceID["protein"], "pqr"),
            (groupGID["indigo"], 21, typeID["gene"], namespaceID["protein"], "qrp"),
            (groupGID["indigo"], 21, typeID["gene"], namespaceID["gene"], "S"),
            (groupGID["violet"], 31, typeID["gene"], namespaceID["protein"], "qrp"),
            (groupGID["violet"], 31, typeID["gene"], namespaceID["protein"], "qrs"),
        ]
        self.addGroupMemberNames(listMember)
        self.log(
            " OK: %d members (%d identifiers)\n"
            % (len(set(m[1] for m in listMember)), len(listMember))
        )

    # update()


# Source_spectrum
