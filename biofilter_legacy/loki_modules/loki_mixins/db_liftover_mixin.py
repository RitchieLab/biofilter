# database_liftover_mixin.py
import bisect
import sys


class DbLiftOverMixin:
    """
    ...
    """

    def hasLiftOverChains(self, oldHG, newHG):
        """
        Check if there are liftOver chains between old and new genome
        assemblies.

        Parameters:
        -----------
        oldHG : str
            Old genome assembly identifier.
        newHG : str
            New genome assembly identifier.

        Returns:
        --------
        int
            Number of liftOver chains found between old and new genome
            assemblies.
        """
        sql = "SELECT COUNT() FROM `db`.`chain` WHERE old_ucschg = ? AND new_ucschg = ?"  # noqa E501
        return max(
            row[0] for row in self._biofilter.db.cursor().execute(sql, (oldHG, newHG))
        )  # noqa E501

    # hasLiftOverChains()

    def _generateApplicableLiftOverChains(
        self, oldHG, newHG, chrom, start, end
    ):  # noqa E501
        """
        Generate applicable liftOver chains for a specific region.

        Parameters:
        -----------
        oldHG : str
            Old genome assembly identifier.
        newHG : str
            New genome assembly identifier.
        chrom : str
            Chromosome name.
        start : int
            Start position of the region.
        end : int
            End position of the region.

        Yields:
        -------
        Tuples containing liftOver chain information for the given region.
            (chain_id, old_chr, score, old_start, old_end, new_start,
            is_fwd, new_chr, old_start, old_end, new_start)
        """
        conv = (oldHG, newHG)
        if conv in self._liftOverCache:
            chains = self._liftOverCache[conv]
        else:
            chains = {"data": {}, "keys": {}}
            sql = """
                SELECT chain_id,
                c.old_chr, c.score, c.old_start, c.old_end, c.new_start,
                c.is_fwd, c.new_chr, cd.old_start, cd.old_end, cd.new_start
                FROM `db`.`chain` AS c
                JOIN `db`.`chain_data` AS cd USING (chain_id)
                WHERE c.old_ucschg=? AND c.new_ucschg=?
                ORDER BY c.old_chr, score DESC, cd.old_start
                """
            for row in self._biofilter.db.cursor().execute(sql, conv):
                chain = (
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[0],
                )  # noqa E501
                chr = row[1]

                if chr not in chains["data"]:
                    chains["data"][chr] = {chain: []}
                    chains["keys"][chr] = [chain]
                elif chain not in chains["data"][chr]:
                    chains["data"][chr][chain] = []
                    chains["keys"][chr].append(chain)

                chains["data"][chr][chain].append((row[8], row[9], row[10]))
            # foreach row

            # Sort the chains by score
            for k in chains["keys"]:
                chains["keys"][k].sort(reverse=True)

            self._liftOverCache[conv] = chains
        # if chains are cached

        for c in chains["keys"].get(chrom, []):
            # if the region overlaps the chain... (1-based, closed intervals)
            if start <= c[2] and end >= c[1]:
                data = chains["data"][chrom][c]
                idx = (
                    bisect.bisect(data, (start, sys.maxsize, sys.maxsize)) - 1
                )  # noqa E501
                while (idx < 0) or (data[idx][1] < start):
                    idx = idx + 1
                while (idx < len(data)) and (data[idx][0] <= end):
                    yield (
                        c[-1],
                        data[idx][0],
                        data[idx][1],
                        data[idx][2],
                        c[4],
                        c[5],
                    )  # noqa E501
                    idx = idx + 1
        # foreach chain

    # _generateApplicableLiftOverChains()

    def _liftOverRegionUsingChains(
        self, label, start, end, extra, first_seg, end_seg, total_mapped_sz
    ):
        """
        Map a region given the 1st and last segment as well as the total
        mapped size.

        Parameters:
        -----------
        label : str
            Label of the region.
        start : int
            Start position of the region.
        end : int
            End position of the region.
        extra : object
            Additional data associated with the region.
        first_seg : tuple
            First segment information.
        end_seg : tuple
            Last segment information.
        total_mapped_sz : int
            Total mapped size of the region.

        Returns:
        --------
        tuple or None
            Mapped region information if mapped successfully, otherwise None.
        """
        mapped_reg = None

        # The front and end differences are the distances from the
        # beginning of the segment.

        # The front difference should be >= 0 and <= size of 1st segment
        front_diff = max(
            0, min(start - first_seg[1], first_seg[2] - first_seg[1])
        )  # noqa E501

        # The end difference should be similar, but w/ last
        end_diff = max(0, min(end - end_seg[1], end_seg[2] - end_seg[1]))

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
            total_mapped_sz
            - front_diff
            - (end_seg[2] - end_seg[1] + 1)
            + end_diff
            + 1  # noqa E501
        )

        if (
            mapped_size / float(end - start + 1) >= 0.95
        ):  # TODO: configurable threshold?
            mapped_reg = (label, first_seg[5], new_start, new_end, extra)

        return mapped_reg

    # _liftOverRegionUsingChains()

    def generateLiftOverRegions(
        self, oldHG, newHG, regions, tally=None, errorCallback=None
    ):
        """
        Generate liftOver regions based on old and new genome assemblies.

        Parameters:
        -----------
        oldHG : str
            Old genome assembly identifier.
        newHG : str
            New genome assembly identifier.
        regions : iterable
            Iterable of regions to be lifted over, where each region is
            represented as a tuple (label, chr, posMin, posMax, extra).
        tally : dict or None, optional
            A dictionary to store the count of lifted and non-lifted
            regions (default is None).
        errorCallback : function or None, optional
            A callback function to handle errors for non-liftable regions
            (default is None).

        Yields:
        -------
        tuple
            Mapped regions in the format (label, chrom, new_start, new_end,
            extra).
        """
        # regions=[ (label,chr,posMin,posMax,extra), ... ]
        oldHG = int(oldHG)
        newHG = int(newHG)
        numNull = numLift = 0
        for region in regions:
            label, chrom, start, end, extra = region

            if start > end:
                start, end = end, start
            is_region = start != end

            # find and apply chains
            mapped_reg = None
            curr_chain = None
            total_mapped_sz = 0
            first_seg = None
            end_seg = None
            for seg in self._generateApplicableLiftOverChains(
                oldHG, newHG, chrom, start, end
            ):
                if curr_chain is None:
                    curr_chain = seg[0]
                    first_seg = seg
                    end_seg = seg
                    total_mapped_sz = seg[2] - seg[1] + 1
                elif seg[0] != curr_chain:
                    mapped_reg = self._liftOverRegionUsingChains(
                        label,
                        start,
                        end,
                        extra,
                        first_seg,
                        end_seg,
                        total_mapped_sz,  # noqa E501
                    )
                    if mapped_reg:
                        break
                    curr_chain = seg[0]
                    first_seg = seg
                    end_seg = seg
                    total_mapped_sz = seg[2] - seg[1] + 1
                else:
                    end_seg = seg
                    total_mapped_sz = total_mapped_sz + seg[2] - seg[1] + 1

            if not mapped_reg and first_seg is not None:
                mapped_reg = self._liftOverRegionUsingChains(
                    label,
                    start,
                    end,
                    extra,
                    first_seg,
                    end_seg,
                    total_mapped_sz,  # noqa E501
                )

            if mapped_reg:
                numLift += 1
                if not is_region:
                    mapped_reg = (
                        mapped_reg[0],
                        mapped_reg[1],
                        mapped_reg[2],
                        mapped_reg[2],
                        extra,
                    )
                yield mapped_reg
            else:
                numNull += 1
                if errorCallback:
                    errorCallback(region)
        # foreach region

        if tally is not None:
            tally["null"] = numNull
            tally["lift"] = numLift

    # generateLiftOverRegions()

    def generateLiftOverLoci(
        self, oldHG, newHG, loci, tally=None, errorCallback=None
    ):  # noqa E501
        """
        Generate liftOver loci based on old and new genome assemblies.

        Parameters:
        -----------
        oldHG : str
            Old genome assembly identifier.
        newHG : str
            New genome assembly identifier.
        loci : iterable
            Iterable of loci to be lifted over, where each locus is
            represented as a tuple (label, chr, pos, extra).
        tally : dict or None, optional
            A dictionary to store the count of lifted and non-lifted loci
            (default is None).
        errorCallback : function or None, optional
            A callback function to handle errors for non-liftable loci
            (default is None).

        Returns:
        --------
        iterable
            Yields new loci in the format (label, chrom, new_pos, extra) for
            each successfully lifted locus.
        """
        # loci=[ (label,chr,pos,extra), ... ]
        regions = ((lc[0], lc[1], lc[2], lc[2], lc[3]) for lc in loci)
        newloci = (
            (r[0], r[1], r[2], r[4])
            for r in self.generateLiftOverRegions(
                oldHG, newHG, regions, tally, errorCallback
            )
        )
        return newloci

    # generateLiftOverLoci()
