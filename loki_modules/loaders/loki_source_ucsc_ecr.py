import itertools
from threading import Thread
import logging
import psutil
import time
from loki_modules import loki_source


class Source_ucsc_ecr(loki_source.Source):
    """
    A class to load the pairwise alignments between species as ECRs from the
    UCSC inter-species alignments
    """

    ##################################################
    # private class data

    _chmList = (
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "X",
        "Y",
        "M",
    )
    _comparisons = {
        "vertebrate": "",
        "placentalMammals": "placental.",
        "primates": "primates.",
    }
    chr_grp_ids = []

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    @classmethod
    def getOptions(cls):
        return {
            "size": "minimum length of an ECR in bases (default: 100)",
            "identity": "minimum identity of an ECR (default: 0.7)",
            "gap": "maximum gap length below the identity threshold (default: 50)",  # noqa E501
        }

    def validateOptions(self, options):
        for o, v in options.items():
            try:
                if o == "size":
                    v = int(v)
                elif o == "identity":
                    v = float(v)
                elif o == "gap":
                    v = int(v)
                elif o == "reverse":  # undocumented debug option
                    v = v.lower()
                    if (
                        (v == "0")
                        or "false".startswith(v)
                        or "no".startswith(v)  # noqa E501
                    ):  # noqa E501
                        v = False
                    elif (
                        (v == "1")
                        or "true".startswith(v)
                        or "yes".startswith(v)  # noqa E501
                    ):  # noqa E501
                        v = True
                    else:
                        return "must be 0/false/no or 1/true/yes"
                else:
                    return "unknown option '%s'" % o
            except ValueError:
                return "Cannot parse '%s' parameter value - given '%s'" % (
                    o,
                    v,
                )  # noqa E501
            options[o] = v
        # foreach option
        return True

    # validateOptions()

    def download(self, options, path):
        remFiles = dict()
        for chm in self._chmList:
            for d, f in self._comparisons.items():
                remFiles[
                    path + "/" + d + ".chr" + chm + ".phastCons.txt.gz"
                ] = (  # noqa E501
                    "/goldenPath/hg19/phastCons46way/"
                    + d
                    + "/chr"
                    + chm
                    + ".phastCons46way."
                    + f
                    + "wigFix.gz"
                )
        # 		self.downloadFilesFromFTP('hgdownload.cse.ucsc.edu', remFiles)
        self.downloadFilesFromHTTP("hgdownload.cse.ucsc.edu", remFiles)

        return list(remFiles.keys())

    # download()

    def update(self, options, path):
        """
        Load the data from all of the files
        UCSC's phastCons files use 1-based coordinates, according to:
                http://genome.ucsc.edu/goldenPath/help/phastCons.html
        Since this matches LOKI's convention, we can store them as-is.
        """
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"UCSC - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            "UCSC - Starting deletion of old records from the database ...",
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "UCSC - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # Add a namespace
        ecr_ns = self.addNamespace("ucsc_ecr")

        # Add a type of "ecr"
        ecr_typeid = self.addType("ecr")

        # Add a type of "ecr_group"
        ecr_group_typeid = self.addType("ecr_group")

        # Make sure the '' ldprofile exists
        ecr_ldprofile_id = self.addLDProfile("", "no LD adjustment")

        # Add a containment relationship
        rel_id = self.addRelationship("contains")

        for sp in self._comparisons:
            self.log(
                "UCSC - Starting the processing ECRs for " + sp + " ...",
                level=logging.INFO,
                indent=2,
                )
            desc = "ECRs for " + sp
            label = "ecr_" + sp

            # Add the group for this species (or comparison)
            ecr_gid = self.addTypedGroups(
                ecr_group_typeid, [(label, desc)]
            )[0]
            self.addGroupNamespacedNames(ecr_ns, [(ecr_gid, label)])

            chr_grp_ids = []
            processThreads = {}
            for ch in self._chmList:
                processThreads[ch] = Thread(
                    target=self.processECRs,
                    args=(
                        sp,
                        ch,
                        chr_grp_ids,
                        ecr_group_typeid,
                        ecr_ns,
                        ecr_typeid,
                        ecr_ldprofile_id,
                        rel_id,
                        options,
                        path,
                    ),
                )
                processThreads[ch].start()

            for ch in self._chmList:
                processThreads[ch].join()

            self.addGroupRelationships(
                ((ecr_gid, c, rel_id, 1) for c in chr_grp_ids)
            )  # noqa E501

            self.log(
                "UCSC - Processing ECRs for " + sp + " completed",
                level=logging.INFO,
                indent=2,
                )

        # store source metadata
        self.setSourceBuilds(None, 19)
        # TODO: check for latest FTP path rather than hardcoded
        # /goldenPath/hg19/phastCons46way/

        # Finalize the process
        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"UCSC - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"UCSC - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )

    # update()

    def processECRs(
        self,
        sp,
        ch,
        chr_grp_ids,
        ecr_group_typeid,
        ecr_ns,
        ecr_typeid,
        ecr_ldprofile_id,
        rel_id,
        options,
        path,
    ):
        ch_id = self._loki.chr_num[ch]
        self.log(
            "UCSC/Thread - Starting the processing Chromosome " + ch + " ...",
            level=logging.INFO,
            indent=2,
            )
        f = self.zfile(path + "/" + sp + ".chr" + ch + ".phastCons.txt.gz")
        curr_band = 1
        num_regions = 0
        desc = "ECRs for " + sp + " on Chromosome " + ch
        chr_grp_ids.append(
            self.addTypedGroups(
                ecr_group_typeid,
                [("ecr_%s_chr%s" % (sp, ch), desc)],  # noqa E501
            )[0]
        )
        self.addGroupNamespacedNames(
            ecr_ns, [(chr_grp_ids[-1], "ecr_%s_chr%s" % (sp, ch))]
        )
        band_grps = []
        grp_rid = {}
        for regions in self.getRegions(f, options):
            label = "ecr_%s_chr%s_band%d" % (sp, ch, curr_band)
            desc = (
                "ECRs for "
                + sp
                + " on Chromosome "
                + ch
                + ", Band %d" % (curr_band,)  # noqa E501
            )
            num_regions += len(regions)

            if regions:
                band_grps.append((label, desc))

            # Add the region itself
            reg_ids = self.addTypedBiopolymers(
                ecr_typeid,
                ((self.getRegionName(sp, ch, r), "") for r in regions),  # noqa E501
            )
            # Add the name of the region
            self.addBiopolymerNamespacedNames(
                ecr_ns,
                zip(
                    reg_ids, (self.getRegionName(sp, ch, r) for r in regions)
                ),  # noqa E501
            )
            # Add the region Boundaries
            # This gives a generator that yields
            # [(region_id, (chrom_id, start, stop)) ... ]
            region_bound_gen = zip(
                ((i,) for i in reg_ids), ((ch_id, r[0], r[1]) for r in regions)
            )
            self.addBiopolymerLDProfileRegions(
                ecr_ldprofile_id,
                (tuple(itertools.chain(*c)) for c in region_bound_gen),  # noqa E501
            )

            if regions:
                grp_rid[band_grps[-1]] = reg_ids

            curr_band += 1

        band_gids = self.addTypedGroups(ecr_group_typeid, band_grps)
        self.addGroupNamespacedNames(
            ecr_ns, zip(band_gids, (r[0] for r in band_grps))
        )  # noqa E501
        gid_rid = []
        for i in range(len(band_gids)):
            gid_rid.extend(
                ((band_gids[i], rid) for rid in grp_rid[band_grps[i]])
            )  # noqa E501

        self.addGroupBiopolymers(gid_rid)

        self.addGroupRelationships(
            ((chr_grp_ids[-1], b, rel_id, 1) for b in band_gids)
        )  # noqa E501

        self.log(
            "UCSC/Thread - Processing Chromosome %s completed (%d regions found in %d bands)"  # noqa E501
            % (ch, num_regions, curr_band - 1),
            level=logging.INFO,
            indent=2,
        )

    def getRegionName(self, species, ch, region):
        """
        Returns a string representation of the name
        """
        return (
            species + ":chr" + ch + ":" + str(region[0]) + "-" + str(region[1])
        )  # noqa E501

    # getRegionName()

    def getRegions(self, f, options):
        # fetch loader options
        minSize = options.get("size", 100)
        minIdent = options.get("identity", 0.7)
        maxGap = options.get("gap", 50)
        reverse = options.get("reverse", False)

        # initialize parser state
        pos = 1
        step = 1
        state = None
        curStart = pos
        curSum = 0.0
        curCount = 0

        # parse the file
        segments = list()
        regions = list()
        EOF = False
        while not EOF:
            declaration = None
            try:
                # parsing can be in one of four states, handled in rough order
                # of frequency; we could cover all cases in one 'for line in
                # f:' loop, but doing extra tests for things that don't change
                # much is ~45% slower
                while True:
                    loopState = state
                    loopPos = pos
                    if (state is False) and (curCount > maxGap):
                        # in a low segment that is already beyond the max gap
                        # length (so we don't care about sum or count anymore)
                        for line in f:
                            v = float(line)
                            if v >= minIdent:
                                state = True
                                break
                            pos += step
                        # for line in f
                    elif state is False:
                        # in a low segment which is still within the max gap
                        # length
                        for line in f:
                            v = float(line)
                            if v >= minIdent:
                                state = True
                                break
                            curSum += v
                            curCount += 1
                            pos += step
                            if curCount > maxGap:
                                break
                        # for line in f
                    elif state is True:
                        # in a high segment
                        for line in f:
                            v = float(line)
                            if v < minIdent:
                                state = False
                                break
                            curSum += v
                            curCount += 1
                            pos += step
                        # for line in f
                    else:
                        # starting a new segment at top of file or after a
                        # data gap (we only have to read 1 value to see what
                        # kind of segment is starting)
                        for line in f:
                            v = float(line)
                            state = v >= minIdent
                            break
                        # for line in f
                    # if

                    # since all states have 'for line in f:' loops, we only
                    # land here for a few reasons
                    if loopState != state:
                        # we changed threshold state; store the segment, reset
                        # the counters and continue
                        segments.append(
                            (curStart, pos - step, curSum, curCount, loopState)
                        )
                        curStart = pos
                        curSum = v
                        curCount = 1
                        pos += step
                    elif loopPos == pos:
                        # we hit EOF; store the segment and process the final
                        # batch
                        segments.append(
                            (curStart, pos - step, curSum, curCount, loopState)
                        )
                        EOF = True
                        break
                    else:
                        # we exceeded the max gap length in a low segment;
                        # process the batch
                        break
                    # if
                # while True
            except ValueError:
                declaration = dict(
                    pair.split("=", 1)
                    for pair in line.strip().split()
                    if "=" in pair  # noqa E501
                )
                if ("start" not in declaration) or ("step" not in declaration):
                    raise Exception(
                        "ERROR: invalid phastcons format: %s" % line
                    )  # noqa E501
                # if the new band picks right up after the old one,
                # ignore it since there was no actual gap in the data
                if int(declaration["start"]) == pos:
                    step = int(declaration["step"])
                    continue
                # store the segment
                segments.append(
                    (curStart, pos - step, curSum, curCount, state)
                )  # noqa E501
            # try/ValueError

            # invert segments if requested
            if reverse:
                for s in range(0, len(segments)):
                    segments[s] = (
                        -segments[s][1],
                        -segments[s][0],
                    ) + segments[  # noqa E501
                        s
                    ][  # noqa E501
                        2:
                    ]  # noqa E501
                segments.reverse()
                tmpregions = regions
                regions = list()

            # set min/max segment indecies to skip leading or trailing low or
            # invalid segments
            sn, sx = 0, len(segments) - 1
            while (sn <= sx) and (segments[sn][4] is not True):
                sn += 1
            while (sn <= sx) and (segments[sx][4] is not True):
                sx -= 1
            # assert ((sn > sx) or ((sx-sn+1)%2)), "segment list size cannot
            # be even (must be hi , hi-lo-hi , etc)"

            # merge applicable high segments according to some metric
            # running-average metric with minSize bugs (original algorithm)
            if 0:
                while sn <= sx:
                    s0, s1 = sn, sn
                    while (s1 < sx) and (
                        (
                            sum(segments[s][2] for s in range(s0, s1 + 2))
                            / sum(segments[s][3] for s in range(s0, s1 + 2))
                        )
                        >= minIdent
                    ):
                        s1 += 2
                    if s1 == sx:
                        if (segments[s1][1] - segments[s0][0]) > minSize:
                            regions.append((segments[s0][0], segments[s1][1]))
                    elif (segments[s1][1] - segments[s0][0]) >= minSize:
                        regions.append((segments[s0][0], segments[s1][1]))
                    sn = s1 + 2
                # while segments to process
            elif 1:  # running-average metric
                while sn <= sx:
                    s0, s1 = sn, sn
                    while (s1 < sx) and (
                        (
                            sum(segments[s][2] for s in range(s0, s1 + 2))
                            / sum(segments[s][3] for s in range(s0, s1 + 2))
                        )
                        >= minIdent
                    ):
                        s1 += 2
                    if (segments[s1][1] - segments[s0][0] + 1) >= minSize:
                        regions.append((segments[s0][0], segments[s1][1]))
                    sn = s1 + 2
                # while segments to process
            elif 0:  # potential-average metric
                while sn <= sx:
                    s0, s1 = sn, sn
                    while (s1 < sx) and (
                        (
                            sum(segments[s][2] for s in range(s0, s1 + 3))
                            / sum(segments[s][3] for s in range(s0, s1 + 3))
                        )
                        >= minIdent
                    ):
                        s1 += 2
                    if (segments[s1][1] - segments[s0][0] + 1) >= minSize:
                        regions.append((segments[s0][0], segments[s1][1]))
                    sn = s1 + 2
                # while segments to process
            elif 0:  # drop-worst metric v1
                partitions = [(sn, sx)] if (sn <= sx) else None
                while partitions:
                    sn, sx = partitions.pop()
                    s0, s1 = sn, sx
                    while (s0 < s1) and (
                        (
                            sum(segments[s][2] for s in range(s0, s1 + 1))
                            / sum(segments[s][3] for s in range(s0, s1 + 1))
                        )
                        < minIdent
                    ):
                        sw = [s1 - 1]
                        for s in range(s1 - 3, s0, -2):
                            if (segments[s][2] + 0.0001) < (
                                segments[sw[0]][2] - 0.0001
                            ):
                                sw = [s]
                            elif (segments[s][2] - 0.0001) <= (
                                segments[sw[0]][2] + 0.0001
                            ):
                                if segments[s][3] > segments[sw[0]][3]:
                                    sw = [s]
                                elif segments[s][3] == segments[sw[0]][3]:
                                    sw.append(s)
                        for s in sw:
                            partitions.append((s + 1, s1))
                            s1 = s - 1
                    # while segments need splitting
                    if (segments[s1][1] - segments[s0][0] + 1) >= minSize:
                        regions.append((segments[s0][0], segments[s1][1]))
                # while segments to process
            elif 0:  # drop-worst metric v2
                partitions = [(sn, sx)] if (sn <= sx) else None
                while partitions:
                    sn, sx = partitions.pop()
                    s0, s1 = sn, sx
                    while (s0 < s1) and (
                        (
                            sum(segments[s][2] for s in range(s0, s1 + 1))
                            / sum(segments[s][3] for s in range(s0, s1 + 1))
                        )
                        < minIdent
                    ):
                        sw = [s1 - 1]
                        for s in range(s1 - 3, s0, -2):
                            if (
                                minIdent * segments[s][3]
                                - segments[s][2]
                                - 0.0001  # noqa E501
                            ) > (  # noqa E501
                                minIdent * segments[sw[0]][3]
                                - segments[sw[0]][2]
                                + 0.0001
                            ):
                                sw = [s]
                            elif (
                                minIdent * segments[s][3]
                                - segments[s][2]
                                + 0.0001  # noqa E501
                            ) >= (
                                minIdent * segments[sw[0]][3]
                                - segments[sw[0]][2]
                                - 0.0001
                            ):
                                if segments[s][3] > segments[sw[0]][3]:
                                    sw = [s]
                                elif segments[s][3] == segments[sw[0]][3]:
                                    sw.append(s)
                        for s in sw:
                            partitions.append((s + 1, s1))
                            s1 = s - 1
                    # while segments need splitting
                    if (segments[s1][1] - segments[s0][0] + 1) >= minSize:
                        regions.append((segments[s0][0], segments[s1][1]))
                # while segments to process
            else:
                raise Exception("ERROR: no segment merge metrics are enabled")
            # if metric
            segments = list()

            # re-invert results if necessary
            if reverse:
                for r in range(len(regions) - 1, -1, -1):
                    tmpregions.append((-regions[r][1], -regions[r][0]))
                regions = tmpregions
                tmpregions = None

            # if we hit a declaration line or EOF, yield this band's regions
            if (declaration or EOF) and regions:
                yield regions
                regions = list()

            # if we hit a declaration line but not EOF, reset the parser state
            if declaration:
                pos = int(declaration["start"])
                step = int(declaration["step"])
                state = None
                curStart = pos
                curSum = 0.0
                curCount = 0
        # while not EOF
