#!/usr/bin/env python

from loki_modules import loki_source


class Source_light(loki_source.Source):

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
                ("group", 0),
            ]
        )
        relationshipID = self.addRelationships(
            [
                ("shade_of",),
                ("greener_than",),
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
            (subtypeID["-"], "red", "normal group"),
            (subtypeID["-"], "green", "unknown member"),
            (subtypeID["-"], "blue", "redundant member name"),
            (subtypeID["-"], "gray", "large parent group"),
        ]
        listGID = self.addTypedGroups(typeID["group"], listGroup)
        groupGID = dict(zip((g[1] for g in listGroup), listGID))
        self.log(" OK: %d groups\n" % len(groupGID))

        # define group names
        self.log("adding group names to the database ...")
        listName = [
            # (group_id,name)
            (groupGID["red"], "red"),
            (groupGID["green"], "green"),
            (groupGID["blue"], "blue"),
            (groupGID["gray"], "gray"),
            (groupGID["gray"], "white"),
        ]
        self.addGroupNamespacedNames(namespaceID["group"], listName)
        self.log(" OK: %d names\n" % len(listName))

        # define group relationships
        self.log("adding group relationships to the database ...")
        listRel = [
            # (group_id,related_group_id,relationship_id,contains)
            (groupGID["red"], groupGID["gray"], relationshipID["shade_of"], -1),
            (groupGID["green"], groupGID["gray"], relationshipID["shade_of"], -1),
            (groupGID["green"], groupGID["blue"], relationshipID["greener_than"], 0),
            (groupGID["blue"], groupGID["gray"], relationshipID["shade_of"], -1),
        ]
        self.addGroupRelationships(listRel)
        self.log(" OK: %d relationships\n" % len(listRel))

        # define group members
        self.log("adding group members to the database ...")
        listMember = [
            # (group_id,member,name)
            (groupGID["red"], 11, "A"),
            (groupGID["red"], 12, "B"),
            (groupGID["green"], 21, "Z"),
            (groupGID["green"], 22, "A"),
            (groupGID["green"], 23, "B"),
            (groupGID["blue"], 31, "A"),
            (groupGID["blue"], 31, "A2"),
            (groupGID["blue"], 32, "C"),
            (groupGID["gray"], 41, "A2"),
            (groupGID["gray"], 42, "B"),
            (groupGID["gray"], 43, "C"),
            (groupGID["gray"], 44, "D"),
            (groupGID["gray"], 45, "E"),
            (groupGID["gray"], 46, "F"),
            (groupGID["gray"], 47, "G"),
        ]
        self.addGroupMemberTypedNamespacedNames(
            typeID["gene"], namespaceID["gene"], listMember
        )
        self.log(
            " OK: %d members (%d identifiers)\n"
            % (len(set(m[1] for m in listMember)), len(listMember))
        )

    # update()


# Source_light
