# #################################################
# UPDATER LIFTOVER MIXIN
# #################################################
import logging


class UpdaterLiftOverMixin:
    def liftOverSNPLoci(self, oldHG, newHG, sourceIDs):
        self.log(
            "lifting over SNP loci from hg%d to hg%d ..." % (oldHG, newHG),  # noqa E501
            level=logging.INFO,
            indent=0,
        )
        self.prepareTableForUpdate("snp_locus")
        cursor = self._db.cursor()
        numLift = numNull = 0
        tally = dict()
        trash = set()

        # identify range of _ROWID_ in snp_locus
        # (two separate queries is faster because a simple MIN() or MAX()
        # only peeks at the index;
        # SQLite isn't clever enough to do that for both at the same time, it
        # does a table scan instead)
        firstRowID = min(
            row[0]
            for row in cursor.execute(
                "SELECT MIN(_ROWID_) FROM `db`.`snp_locus`"  # noqa E501
            )
        )
        lastRowID = max(
            row[0]
            for row in cursor.execute(
                "SELECT MAX(_ROWID_) FROM `db`.`snp_locus`"  # noqa E501
            )
        )

        # define a callback to store loci that can't be lifted over,
        # for later deletion
        def errorCallback(region):
            trash.add((region[0],))

        # we can't SELECT and UPDATE the same table at the same time,
        # so read in batches of 2.5 million at a time based on _ROWID_
        minRowID = firstRowID
        maxRowID = minRowID + 2500000 - 1
        while minRowID <= lastRowID:
            sql = "SELECT _ROWID_, chr, pos, NULL FROM `db`.`snp_locus`"
            sql += " WHERE (_ROWID_ BETWEEN ? AND ?) AND source_id IN (%s)" % (
                ",".join(str(i) for i in sourceIDs)
            )
            oldLoci = list(cursor.execute(sql, (minRowID, maxRowID)))
            newLoci = self._loki.generateLiftOverLoci(
                oldHG, newHG, oldLoci, tally, errorCallback
            )
            sql = (
                "UPDATE OR REPLACE `db`.`snp_locus` "
                "SET chr = ?2, pos = ?3 "
                "WHERE _ROWID_ = ?1"
            )

            cursor.executemany(sql, newLoci)
            numLift += tally["lift"]
            numNull += tally["null"]
            if trash:
                cursor.executemany(
                    "DELETE FROM `db`.`snp_locus` WHERE _ROWID_ = ?", trash
                )
                trash.clear()
            minRowID = maxRowID + 1
            maxRowID = minRowID + 2500000 - 1
        # foreach batch

        self.log(
            " OK: %d loci lifted over, %d dropped\n" % (numLift, numNull),
            level=logging.INFO,
            indent=0,
            )

    def liftOverRegions(self, oldHG, newHG, sourceIDs):
        self.log(
            "lifting over regions from hg%d to hg%d ..." % (oldHG, newHG),
            level=logging.INFO,
            indent=0,
            )
        self.prepareTableForUpdate("biopolymer_region")
        cursor = self._db.cursor()
        numLift = numNull = 0
        tally = dict()
        trash = set()

        # identify range of _ROWID_ in biopolymer_region
        # (two separate queries is faster because a simple MIN() or MAX() only
        # peeks at the index;
        # SQLite isn't clever enough to do that for both at the same time, it
        # does a table scan instead)
        firstRowID = min(
            row[0]
            for row in cursor.execute(
                "SELECT MIN(_ROWID_) FROM `db`.`biopolymer_region`"
            )
        )
        lastRowID = max(
            row[0]
            for row in cursor.execute(
                "SELECT MAX(_ROWID_) FROM `db`.`biopolymer_region`"
            )
        )

        # define a callback to store regions that can't be lifted over, for
        # later deletion
        def errorCallback(region):
            trash.add((region[0],))

        # we can't SELECT and UPDATE the same table at the same time,
        # so read in batches of 2.5 million at a time based on _ROWID_
        # (for regions this will probably be all of them in one go, but just
        # in case)
        minRowID = firstRowID
        maxRowID = minRowID + 2500000 - 1
        while minRowID <= lastRowID:
            sql = (
                "SELECT _ROWID_, chr, posMin, posMax, NULL "
                "FROM `db`.`biopolymer_region`"
            )
            sql += " WHERE (_ROWID_ BETWEEN ? AND ?) AND source_id IN (%s)" % (
                ",".join(str(i) for i in sourceIDs)
            )
            oldRegions = list(cursor.execute(sql, (minRowID, maxRowID)))
            newRegions = self._loki.generateLiftOverRegions(
                oldHG, newHG, oldRegions, tally, errorCallback
            )
            sql = (
                "UPDATE OR REPLACE `db`.`biopolymer_region` "
                "SET chr = ?2, posMin = ?3, posMax = ?4 "
                "WHERE _ROWID_ = ?1 AND (1 OR ?5)"
            )
            cursor.executemany(sql, newRegions)
            numLift += tally["lift"]
            numNull += tally["null"]
            if trash:
                cursor.executemany(
                    "DELETE FROM `db`.`biopolymer_region` WHERE _ROWID_ = ?",
                    trash,  # noqa E501
                )
                trash.clear()
            minRowID = maxRowID + 1
            maxRowID = minRowID + 2500000 - 1
        # foreach batch

        self.log(
            " OK: %d regions lifted over, %d dropped\n"
            % (numLift, numNull),
            level=logging.INFO,
            indent=0,
        )
