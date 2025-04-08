from threading import Thread
import logging
import psutil
import time
from loki_modules import loki_source
from loki_mixins import SourceUtilMixin
import gc


class Source_ucsc_ecr(loki_source.Source):
    """
    A class to load the pairwise alignments between species as ECRs from the
    UCSC inter-species alignments
    """

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
        # 75 files to download!!
        self.downloadFilesFromHTTP("hgdownload.cse.ucsc.edu", remFiles)

        return list(remFiles.keys())

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
        self.deleteAll()  # will drop by Source ID
        self.log(
            "UCSC - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        # Global data to UCSR ECRs
        # ------------------------
        ecr_ns = self.addNamespace("ucsc_ecr")
        ecr_typeid = self.addType("ecr")
        ecr_group_typeid = self.addType("ecr_group")
        ecr_ldprofile_id = self.addLDProfile("", "no LD adjustment")
        rel_id = self.addRelationship("contains")

        # sp = 3 keys # ch = 25 keys
        for sp in self._comparisons:
            self.log(
                "UCSC - Starting the processing ECRs for " + sp + " ...",
                level=logging.INFO,
                indent=2,
            )

            # PROCESS FASE
            # ============

            # Variables for parallel processing
            threads = []
            list_n2 = []
            list_n3 = []
            list_n4 = []

            # Run parallel processing for each chromosome
            for ch in self._chmList:
                thread = self.ProcessingThread(
                    sp,
                    ch,
                    self._loki.chr_num[ch],
                    options,
                    path,
                )
                threads.append(thread)
                thread.start()
            # Collect the results from the threads
            for thread in threads:
                thread.join()
                list_n2.extend(thread.list_n2)
                list_n3.extend(thread.list_n3)
                list_n4.extend(thread.list_n4)

            # INGESTIONS FASE
            # ===============

            # N1 Ingestions: Comparisons Level
            # --------------------------------------------------------------------------------
            # Structure: ecr_{comparisons}
            desc_n1 = "ECRs for " + sp
            label_n1 = "ecr_" + sp
            list_n1 = []  # [label, description]
            list_n1.append([label_n1, desc_n1])

            # GROUP TABLE: [group_id, type_id, label, description, source_id]
            ecr_gid = self.addTypedGroups(ecr_group_typeid, list_n1)[0]

            # GROUP_NAME TABLE: [group_id, namespace_id, name, source_id]
            self.addGroupNamespacedNames(ecr_ns, [(ecr_gid, list_n1[0][0])])

            # N2 Ingestions: Chromosome Level
            # --------------------------------------------------------------------------------
            # Structure: ect_{comparisons}_chr{x}
            # GROUP TABLE: [group_id, type_id, label, description, source_id]
            list_n2_grp_ids = []
            list_n2_grp_ids = self.addTypedGroups(ecr_group_typeid, list_n2)
            # list_n2: [group_id, label, description]
            list_n2 = [
                [group_id] + list(row)
                for group_id, row in zip(list_n2_grp_ids, list_n2)
            ]

            # GROUP_NAME TABLE: [group_id, namespace_id, name, source_id]
            self.addGroupNamespacedNames(
                ecr_ns, [(row[0], row[1]) for row in list_n2]
            )  # noqa E501

            # GROUP_GROUP TABLE:
            # [group_id, related_group_id, relationship_id, direction, contains, source_id]  # noqa E501
            # N1 <--> N2
            self.addGroupRelationships(
                ((ecr_gid, row[0], rel_id, 1) for row in list_n2)
            )

            # N3 Ingestions: Band Level
            # --------------------------------------------------------------------------------
            # Structure: ect_{comparisons}_chr{x}_band{y}
            # GROUP N3: [group_id, type_id, label, descrition, source_id]
            # list_n3 [grp_n2_label, grp_label_n3, desc_n3]

            list_n3_grp_ids = []
            list_n3_grp_ids = self.addTypedGroups(
                ecr_group_typeid, [(row[1], row[2]) for row in list_n3]
            )

            # GROUP_NAME TABLE: [group_id, namespace_id, name, source_id]
            # list_n3: [grp_id_n3, grp_n2_label, grp_label_n3, desc_n3]
            list_n3 = [
                [group_id] + list(row)
                for group_id, row in zip(list_n3_grp_ids, list_n3)
            ]  # noqa E501
            self.addGroupNamespacedNames(
                ecr_ns, [(row[0], row[2]) for row in list_n3]
            )  # noqa E501

            # GROUP_GROUP TABLE:
            # [group_id, related_group_id, relationship_id, direction, contains, source_id]  # noqa E501
            # create 2 dictionaries to map 'label' -> group_id
            dict_n2 = {row[1]: row[0] for row in list_n2}
            dict_n3 = {row[2]: row[0] for row in list_n3}

            list_g2 = []

            for row in list_n3:
                parent_label = row[1]  # group_n2_label
                child_label = row[2]  # group_label_n3
                parent_id = dict_n2.get(parent_label)  # get ID of parent (n2)
                child_id = dict_n3.get(child_label)  # get ID of child (n3)

                if parent_id and child_id:  # check if IDs exists
                    list_g2.append(
                        (parent_id, parent_label, child_id, child_label)
                    )  # noqa E501
                    # NOTE: To save memory, drop labels and use only IDs

            # N2 <--> N3
            self.addGroupRelationships(
                ((row[0], row[2], rel_id, 1) for row in list_g2)
            )  # noqa E501

            # N4 Ingestions: Biopolymer Level
            # --------------------------------------------------------------------------------
            # Structure: ect_{comparisons}_chr{x}_band{y}_{region}

            # BIOPOLYMER TABLE:
            # [biopolymer_id, type_id, label, description, source_id]  # noqa E501
            list_n4_bio_ids = []
            list_n4_bio_ids = self.addTypedBiopolymers(
                ecr_typeid,
                [(row[1], "") for row in list_n4],
            )
            # list_n4:
            # [biopolymer_id, label_grp, label, description, chr_num, start, stop]  # noqa E501
            list_n4 = [
                [biopolymer_id] + list(row)
                for biopolymer_id, row in zip(list_n4_bio_ids, list_n4)
            ]

            # BIOPOLYMER_NAME TABLE:
            # [biopolymer_id, namespace_id, name, source_id]
            self.addBiopolymerNamespacedNames(
                ecr_ns, [(row[0], row[2]) for row in list_n4]
            )

            # BIOPOLYMER_REGION TABLE:
            # [biopolymer_id, ldprofile_id, chr_num, posMin, posMax, source_id]
            self.addBiopolymerLDProfileRegions(
                ecr_ldprofile_id,
                [(row[0], row[3], row[4], row[5]) for row in list_n4],  #
            )

            # GROUP_BIOPOLYMER TABLE:
            # [group_id, biopolymer_id, specificity, implication, quality, source_id]  # noqa E501
            # Create a dictionary to map 'label' -> group_id
            group_dict = {row[2]: row[0] for row in list_n3}
            list_n4_grp_bio_ids = []

            for biopolymer in list_n4:
                biopolymer_id = biopolymer[0]
                label = biopolymer[1]
                # Get the group_id using the label
                group_id = group_dict.get(label)  # TODO if not found?
                # Append the tuple to the list
                if group_id:
                    list_n4_grp_bio_ids.append((group_id, biopolymer_id))

            self.addGroupBiopolymers(list_n4_grp_bio_ids)

            self.log(
                "UCSC - Processing ECRs for " + sp + " completed",
                level=logging.INFO,
                indent=2,
            )

        # store source metadata
        self.setSourceBuilds(None, 19)

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

    class ProcessingThread(Thread, SourceUtilMixin):
        def __init__(
            self,
            sp,
            ch,
            ch_id,
            options,
            path,
        ):
            super().__init__()
            self.sp = sp
            self.ch = ch
            self.ch_id = ch_id
            self.options = options
            self.path = path

            self.list_n2 = []
            self.list_n3 = []
            self.list_n4 = []

        def run(self):
            try:
                # N2 PROCESS: Chromossome Level
                desc_n2 = "ECRs for " + self.sp + " on Chromosome " + self.ch
                label_n2 = "ecr_" + self.sp + "_chr" + self.ch

                self.list_n2.append([label_n2, desc_n2])  # noqa E501

                curr_band = 1
                num_regions = 0

                # Read the source file
                f = self.zfile(
                    self.path
                    + "/"
                    + self.sp
                    + ".chr"
                    + self.ch
                    + ".phastCons.txt.gz"  # noqa E501
                )

                # Variables for processing Biopolymer
                for regions in self.getRegions(f, self.options):
                    # N3 PROCESS: Band Level
                    # ecr_{comparisons}_chr{x}_brand{y}
                    label_grp_n3 = "ecr_%s_chr%s_band%d" % (
                        self.sp,
                        self.ch,
                        curr_band,
                    )  # noqa E501
                    desc_n3 = (
                        "ECRs for "
                        + self.sp
                        + " on Chromosome "
                        + self.ch
                        + ", Band %d" % (curr_band,)
                    )
                    self.list_n3.append((label_n2, label_grp_n3, desc_n3))

                    num_regions += len(regions)

                    # N4 PROCESS: Biopolymer Level
                    # ecr_{comparisons}_chr{x}_{region}
                    for r in regions:
                        start = r[0]
                        stop = r[1]
                        label_n3 = self.getRegionName(self.sp, self.ch, r)
                        self.list_n4.append(
                            (
                                label_grp_n3,
                                label_n3,
                                self.ch_id,
                                start,
                                stop,
                            )  # noqa E501
                        )

                    curr_band += 1

            finally:
                # Clean up memory
                if "f" in locals() and f:
                    f.close()
                    del f
                gc.collect()
                print(f"Thread {self.ch}: clean up memory")

            print(f"Thread {self.ch}: processed {num_regions} regions")

        def getRegionName(self, species, ch, region):
            """
            Returns a string representation of the name
            """
            return (
                species
                + ":chr"
                + ch
                + ":"
                + str(region[0])
                + "-"
                + str(region[1])  # noqa 501
            )  # noqa E501

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
                    # parsing can be in one of four states, handled in rough
                    # order of frequency; we could cover all cases in one 'for
                    # line in f:' loop, but doing extra tests for things that
                    # don't change much is ~45% slower
                    while True:
                        loopState = state
                        loopPos = pos
                        if (state is False) and (curCount > maxGap):
                            # in a low segment that is already beyond the max
                            # gap length (so we don't care about sum or count
                            # anymore)
                            for line in f:
                                v = float(line)
                                if v >= minIdent:
                                    state = True
                                    break
                                pos += step
                            # for line in f
                        elif state is False:
                            # in a low segment which is still within the max
                            # gap length
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
                            # data gap (we only have to read 1 value to see
                            # what kind of segment is starting)
                            for line in f:
                                v = float(line)
                                state = v >= minIdent
                                break
                            # for line in f
                        # if

                        # since all states have 'for line in f:' loops, we only
                        # land here for a few reasons
                        if loopState != state:
                            # we changed threshold state; store the segment,
                            # reset the counters and continue
                            segments.append(
                                (
                                    curStart,
                                    pos - step,
                                    curSum,
                                    curCount,
                                    loopState,
                                )  # noqa E501
                            )
                            curStart = pos
                            curSum = v
                            curCount = 1
                            pos += step
                        elif loopPos == pos:
                            # we hit EOF; store the segment and process the
                            # final batch
                            segments.append(
                                (
                                    curStart,
                                    pos - step,
                                    curSum,
                                    curCount,
                                    loopState,
                                )  # noqa E501
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
                    if ("start" not in declaration) or (
                        "step" not in declaration
                    ):  # noqa E501
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

                # set min/max segment indecies to skip leading or trailing low
                # or invalid segments
                sn, sx = 0, len(segments) - 1
                while (sn <= sx) and (segments[sn][4] is not True):
                    sn += 1
                while (sn <= sx) and (segments[sx][4] is not True):
                    sx -= 1
                # assert ((sn > sx) or ((sx-sn+1)%2)), "segment list size
                # cannot be even (must be hi , hi-lo-hi , etc)"

                # merge applicable high segments according to some metric
                # running-average metric with minSize bugs (original algorithm)
                if 0:
                    while sn <= sx:
                        s0, s1 = sn, sn
                        while (s1 < sx) and (
                            (
                                sum(segments[s][2] for s in range(s0, s1 + 2))
                                / sum(
                                    segments[s][3] for s in range(s0, s1 + 2)
                                )  # noqa E501
                            )
                            >= minIdent
                        ):
                            s1 += 2
                        if s1 == sx:
                            if (segments[s1][1] - segments[s0][0]) > minSize:
                                regions.append(
                                    (segments[s0][0], segments[s1][1])
                                )  # noqa E501
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
                                / sum(
                                    segments[s][3] for s in range(s0, s1 + 2)
                                )  # noqa E501
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
                                / sum(
                                    segments[s][3] for s in range(s0, s1 + 3)
                                )  # noqa E501
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
                                / sum(
                                    segments[s][3] for s in range(s0, s1 + 1)
                                )  # noqa E501
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
                                / sum(
                                    segments[s][3] for s in range(s0, s1 + 1)
                                )  # noqa E501
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
                    self.log(
                        "ERROR: no segment merge metrics are enabled",
                        level=logging.ERROR,
                        indent=2,
                    )
                    raise Exception("ERROR: no segment merge metrics are enabled")  #

                # if metric
                segments = list()

                # re-invert results if necessary
                if reverse:
                    for r in range(len(regions) - 1, -1, -1):
                        tmpregions.append((-regions[r][1], -regions[r][0]))
                    regions = tmpregions
                    tmpregions = None

                # if we hit a declaration line or EOF, yield this band's
                # regions
                if (declaration or EOF) and regions:
                    yield regions
                    regions = list()

                # if we hit a declaration line but not EOF, reset the parser
                # state
                if declaration:
                    pos = int(declaration["start"])
                    step = int(declaration["step"])
                    state = None
                    curStart = pos
                    curSum = 0.0
                    curCount = 0
            # while not EOF
