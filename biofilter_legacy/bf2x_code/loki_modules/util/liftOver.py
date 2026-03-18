#!/usr/bin/env python

import sys
import bisect

##################################################
# Note on included docstring
# Code was created over 10+ years by several developers
# ChatGPT was used to generate docstring in June 2024 to help with legacy code interpretation
# Docstring has not been inspected line by line
##################################################


class liftOver(object):
    """
    A class for lifting over genomic coordinates between assemblies.

    This class provides methods to map genomic regions from one assembly
    (old_ucschg) to another (new_ucschg) using chain data stored in a database.

    Attributes:
    -----------
    _db : loki_db.Database
            Instance of the LOKI database used for storing chain data.
    _old_ucschg : int
            Version of the old assembly (e.g., 19).
    _new_ucschg : int
            Version of the new assembly (e.g., 38).
    _cached : bool
            Flag indicating whether to use cached chain data for optimization.
    _minFrac : float
            Minimum fraction of the region that must be mapped for successful liftOver.

    Methods:
    --------
    __init__(db, old_ucschg, new_ucschg, cached=False):
            Initializes a liftOver object with the provided parameters.

    _initChains():
            Initializes the cached chain data from the database.

    _findChains(chrom, start, end):
            Finds chain segments that overlap with the given region.

    liftRegion(chrom, start, end):
            Lifts a genomic region from old_ucschg to new_ucschg assembly.

    _mapRegion(region, first_seg, end_seg, total_mapped_sz):
            Maps a region using chain segment data.

    Notes:
    ------
    This class assumes chain data is stored in the LOKI database and uses
    this data to perform liftOver operations between assemblies.
    """

    def __init__(self, db, old_ucschg, new_ucschg, cached=False):
        """
        Initializes a liftOver object with the provided parameters.

        Parameters:
        -----------
        db : loki_db.Database
                Instance of the LOKI database containing chain data.
        old_ucschg : int
                Version of the old assembly (e.g., 19).
        new_ucschg : int
                Version of the new assembly (e.g., 38).
        cached : bool, optional
                Flag indicating whether to use cached chain data (default is False).
        """
        # db is a loki_db.Database object
        self._db = db
        self._old_ucschg = old_ucschg
        self._new_ucschg = new_ucschg
        self._cached = cached
        self._minFrac = 0.95
        if self._cached:
            self._cached_data = {}
            self._cached_keys = {}
            self._chainData = self._initChains()

    def _initChains(self):
        """
        Initializes the cached chain data from the database.

        This method constructs a cached representation of chain data for
        optimized region mapping.
        """
        for row in self._db._db.cursor().execute(
            "SELECT chain_id, old_chr, score, chain.old_start, "
            + "chain.old_end, chain.new_start, is_fwd, new_chr, "
            + "chain_data.old_start, chain_data.old_end, chain_data.new_start "
            + "FROM db.chain INNER JOIN db.chain_data USING (chain_id) "
            + "WHERE old_ucschg=? AND new_ucschg=?"
            + "ORDER BY old_chr, score DESC, chain_data.old_start",
            (self._old_ucschg, self._new_ucschg),
        ):

            chain = (row[2], row[3], row[4], row[5], row[6], row[7], row[0])
            chr = row[1]

            if chr not in self._cached_data:
                self._cached_data[chr] = {chain: []}
                self._cached_keys[chr] = [chain]
            elif chain not in self._cached_data[chr]:
                self._cached_data[chr][chain] = []
                self._cached_keys[chr].append(chain)

            self._cached_data[chr][chain].append((row[8], row[9], row[10]))

        # Sort the chains by score
        for k in self._cached_keys:
            self._cached_keys[k].sort(reverse=True)

    def _findChains(self, chrom, start, end):
        """
        Finds chain segments that overlap with the given region.

        Parameters:
        -----------
        chrom : str
                Chromosome name or identifier.
        start : int
                Start position of the region.
        end : int
                End position of the region.

        Yields:
        ------
        tuple:
                Chain segment details including chain_id, old_start, old_end,
                new_start, is_fwd, new_chr.

        Notes:
        ------
        This method queries the database or uses cached data to find chain
        segments that overlap with the specified region.
        """
        if not self._cached:
            for row in self._db._db.cursor().execute(
                "SELECT chain.chain_id, chain_data.old_start, chain_data.old_end, chain_data.new_start, is_fwd, new_chr "
                + "FROM chain INNER JOIN chain_data ON chain.chain_id = chain_data.chain_id "
                + "WHERE old_ucschg=? AND new_ucschg=? AND old_chr=? AND chain.old_end>=? AND chain.old_start<? AND chain_data.old_end>=? AND chain_data.old_start<? "
                + "ORDER BY score DESC",
                (self._old_ucschg, self._new_ucschg, chrom, start, end, start, end),
            ):
                yield row
        else:
            for c in self._cached_keys.get(chrom, []):
                # if the region overlaps the chain...
                if start <= c[2] and end >= c[1]:
                    data = self._cached_data[chrom][c]
                    idx = bisect.bisect(data, (start, sys.maxsize, sys.maxsize))
                    if idx:
                        idx = idx - 1

                    if idx < len(data) - 1 and start == data[idx + 1]:
                        idx = idx + 1

                    while idx < len(data) and data[idx][0] < end:
                        yield (
                            c[-1],
                            data[idx][0],
                            data[idx][1],
                            data[idx][2],
                            c[4],
                            c[5],
                        )
                        idx = idx + 1

    def liftRegion(self, chrom, start, end):
        """
        Lifts a genomic region from old_ucschg to new_ucschg assembly.

        Parameters:
        -----------
        chrom : str
                Chromosome name or identifier.
        start : int
                Start position of the region.
        end : int
                End position of the region.

        Returns:
        --------
        tuple or None:
                Mapped region (new_chr, new_start, new_end) or None if unable to map.

        Notes:
        ------
        This method uses chain data to map the specified genomic region from
        the old_ucschg assembly to the new_ucschg assembly.
        """
        # We need to actually lift regions to detect dropped sections
        is_region = True

        # If the start and end are swapped, reverse them, please
        if start > end:
            (start, end) = (end, start)
        elif start == end:
            is_region = False
            end = start + 1

        ch_list = self._findChains(chrom, start, end)

        # This will be a tuple of (start, end) of the mapped region
        # If the function returns "None", then it was unable to map
        # the region into the new assembly
        mapped_reg = None

        curr_chain = None

        total_mapped_sz = 0
        first_seg = None
        end_seg = None
        for seg in ch_list:
            if curr_chain is None:
                curr_chain = seg[0]
                first_seg = seg
                end_seg = seg
                total_mapped_sz = seg[2] - seg[1]
            elif seg[0] != curr_chain:
                mapped_reg = self._mapRegion(
                    (start, end), first_seg, end_seg, total_mapped_sz
                )
                if not mapped_reg:
                    first_seg = seg
                    end_seg = seg
                    total_mapped_sz = seg[2] - seg[1]
                else:
                    break
            else:
                end_seg = seg
                total_mapped_sz = total_mapped_sz + seg[2] - seg[1]

        if not mapped_reg and first_seg is not None:
            mapped_reg = self._mapRegion(
                (start, end), first_seg, end_seg, total_mapped_sz
            )

        if mapped_reg and not is_region:
            mapped_reg = (mapped_reg[0], mapped_reg[1], mapped_reg[1])  # bug?

        return mapped_reg

    def _mapRegion(self, region, first_seg, end_seg, total_mapped_sz):
        """
        Maps a region using chain segment data.

        Parameters:
        -----------
        region : tuple
                Genomic region (start, end) to map.
        first_seg : tuple
                First segment of the chain (chain_id, old_start, old_end, new_start, is_fwd, new_chr).
        end_seg : tuple
                Last segment of the chain (chain_id, old_start, old_end, new_start, is_fwd, new_chr).
        total_mapped_sz : int
                Total size of mapped segments.

        Returns:
        --------
        tuple or None:
                Mapped region (new_chr, new_start, new_end) or None if unable to map.

        Notes:
        ------
        This method calculates the mapped region based on the chain segments
        and verifies if the mapped fraction meets the minimum required.
        """
        mapped_reg = None

        # The front and end differences are the distances from the
        # beginning of the segment.

        # The front difference should be >= 0 and <= size of 1st segment
        front_diff = max(0, min(region[0] - first_seg[1], first_seg[2] - first_seg[1]))

        # The end difference should be similar, but w/ last
        end_diff = max(0, min(region[1] - end_seg[1], end_seg[2] - end_seg[1]))

        # Now, if we are moving forward, we add the difference
        # to the new_start, backward, we subtract
        # Also, at this point, if backward, swap start/end
        if first_seg[4]:
            new_start = first_seg[3] + front_diff
            new_end = end_seg[3] + end_diff
        else:
            new_start = end_seg[3] - end_diff
            new_end = first_seg[3] - front_diff

        # old_startHere, detect if we have mapped a sufficient fraction
        # of the region.  liftOver uses a default of 95%
        mapped_size = (
            total_mapped_sz - front_diff - (end_seg[2] - end_seg[1]) + end_diff
        )

        if mapped_size / float(region[1] - region[0]) >= self._minFrac:
            mapped_reg = (first_seg[5], new_start, new_end)

        return mapped_reg


if __name__ == "__main__":
    from loki_modules import loki_db

    if len(sys.argv) < 5:
        print(
            "usage: %s <input> <lokidb> <output> <unmap> [oldhg=19] [newhg=38]"
            % (sys.argv[0],)
        )
        sys.exit(2)

    db = loki_db.Database(sys.argv[2])

    old = int(sys.argv[5]) if (len(sys.argv) > 5) else 19
    new = int(sys.argv[6]) if (len(sys.argv) > 6) else 38
    # lo = liftOver(db, old, new, False)
    f = sys.stdin if (sys.argv[1] == "-") else open(sys.argv[1], "r")
    m = sys.stdout if (sys.argv[3] == "-") else open(sys.argv[3], "w")
    u = sys.stderr if (sys.argv[4] == "-") else open(sys.argv[4], "w")

    def generateInputs(f):
        """
        Generates input data for liftOver region conversion.

        Parameters:
        -----------
        f : file object
                Input file object containing genomic coordinates.

        Yields:
        ------
        tuple:
                Tuple containing processed genomic region information:
                (formatted_line, chromosome_number, start_position, end_position, None).

        Notes:
        ------
        This function reads lines from the input file object 'f', processes
        genomic coordinates, replaces spaces and tabs with colons, adjusts
        chromosome names, and retrieves chromosome numbers from 'db'.
        """
        for l in f:
            wds = l.split()
            if wds[0].lower().startswith("chr"):
                wds[0] = wds[0][3:]
            yield (
                l.strip().replace(" ", ":").replace("\t", ":"),
                db.chr_num.get(wds[0], -1),
                int(wds[1]),
                int(wds[2]),
                None,
            )

    def errorCallback(r):
        """
        Error callback function for handling liftOver errors.

        Parameters:
        -----------
        r : tuple
                Tuple containing error details to be processed.

        Notes:
        ------
        This function prints the error details to the stderr stream 'u'
        in a tab-separated format.
        """
        print("\t".join(str(c) for c in r), end="", file=u)

    for r in db.generateLiftOverRegions(
        old, new, generateInputs(f), errorCallback=errorCallback
    ):
        print(
            "chr%s\t%s\t%d\t%d" % (db.chr_name.get(r[1], r[1]), r[0], r[2], r[3]),
            end="",
            file=m,
        )
