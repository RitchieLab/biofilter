import itertools
import psutil
import time
import os
import re
import logging
import urllib.request as urllib2
from loki_modules import loki_source


class Source_chainfiles(loki_source.Source):

    _reFile = re.compile(
        r"^hg([0-9]+)tohg([0-9]+)\.over\.chain\.gz$", re.IGNORECASE
    )  # noqa E501
    _reFileName = r"hg([0-9]+)ToHg([0-9]+)\.over\.chain\.gz"

    _reNum = ("4", "10", "11", "12", "13", "15", "16", "17", "18", "19", "38")

    @classmethod
    def getVersionString(cls):
        return "3.0.0 (2025-01-01)"

    def download(self, options, path):
        remFiles = {}
        for i in self._reNum:
            urlpath = urllib2.urlopen(
                "http://hgdownload.cse.ucsc.edu/goldenPath/hg%s/liftOver" % i
            )
            string = urlpath.read().decode("utf-8")
            onlyfiles = list(set(re.findall(self._reFileName, string)))
            for j in onlyfiles:
                if i == j[0]:
                    filenames = "hg" + j[0] + "ToHg" + j[1] + ".over.chain.gz"
                    remFiles[path + "/" + filenames] = (
                        "/goldenPath/hg" + i + "/liftOver/" + filenames
                    )

        self.downloadFilesFromHTTP("hgdownload.cse.ucsc.edu", remFiles)
        # BUG: We need think how to haldle errors in downloadFilesFromHTTP!
        # BUG: Now log error and runn process (deleteAll and update w/error)

        return list(remFiles.keys())

    def update(self, options, path):
        # clear out all old data from this source
        start_time = time.time()
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)  # in MB

        self.log(
            f"Chainfiles - Starting Data Ingestion (inicial memory {memory_before:.2f} MB) ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )

        self.log(
            "Chainfiles - Starting deletion of old records from the database ...",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.deleteAll()
        self.log(
            "Chainfiles - Old records deletion completed",
            level=logging.INFO,
            indent=2,
        )

        for fn in os.listdir(path):
            match = self._reFile.match(fn)
            if not match:
                continue
            old_ucschg = int(match.group(1))
            new_ucschg = int(match.group(2))
            self.log(
                "Chainfiles - Parsing chains for hg%d -> hg%d ..."
                % (old_ucschg, new_ucschg),  # noqa: E501
                level=logging.INFO,
                indent=2,
            )  # noqa: E501

            f = self.zfile(path + "/" + fn)

            is_hdr = True
            is_valid = True
            chain_hdrs = []
            chain_data = []
            curr_data = []
            for line in f:
                if is_hdr:
                    if line:
                        try:
                            chain_hdrs.append(self._parseChain(line))
                        except:  # noqa: E722
                            is_valid = False
                        is_hdr = False
                elif line:
                    if is_valid:
                        curr_data.append(line)
                else:
                    if is_valid:
                        chain_data.append(
                            self._parseData(
                                chain_hdrs[-1], "\n".join(curr_data)
                            )  # noqa: E501
                        )
                    is_valid = True
                    curr_data = []
                    is_hdr = True

            hdr_ids = self.addChains(old_ucschg, new_ucschg, chain_hdrs)

            # Now, I want to take my list of IDs and my list of list of
            # tuples and convert them into a list of tuples suitable for
            # entering in the chain_data table
            chain_id_data = zip(hdr_ids, chain_data)
            chain_data_itr = (
                tuple(itertools.chain((chn[0],), seg))
                for chn in chain_id_data
                for seg in chn[1]
            )

            self.addChainData(chain_data_itr)

            self.log(
                "Chainfiles - Parsing chains completed",
                level=logging.INFO,
                indent=2,
            )
        # for fn in dir

        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60  # time in minutes
        memory_after = process.memory_info().rss / (1024 * 1024)  # mem in MB
        self.log(
            f"Chainfiles - Final memory: {memory_after:.2f} MB. Alocated memory: {memory_after - memory_before:.2f} MB.",  # noqa: E501
            level=logging.INFO,
            indent=2,
        )
        self.log(
            f"Chainfiles - Update completed in {elapsed_time_minutes:.2f} minutes.",  # noqa: E501
            level=logging.CRITICAL,
            indent=2,
        )

    # update()

    def _parseChain(self, chain_hdr):
        """
        Parses the chain header to extract the information required
        for insertion into the database.
        UCSC chain files use 0-based half-open intervals according to:
        https://genome.ucsc.edu/goldenPath/help/chain.html
        Since LOKI uses 1-based closed intervals, we add 1 to start positions.
        """

        # get the 1st line
        hdr = chain_hdr.strip().split("\n")[0].strip()

        # Parse the first line
        # "chain" score oldChr oldSize oldDir oldStart oldEnd newChr newSize
        # newDir newStart newEnd id
        wds = hdr.split()

        if wds[0] != "chain":
            raise Exception("Not a valid chain file")

        if wds[2][3:] not in self._loki.chr_num:
            raise Exception(
                "Could not find chromosome: " + wds[2][3:] + "->" + wds[7][3:]
            )

        is_fwd = wds[9] == "+"
        if is_fwd:
            new_start = int(wds[10]) + 1
            new_end = int(wds[11])
        else:
            # NOTE: If we're going backward, this will mean that
            # end < start
            new_start = int(wds[8]) - int(wds[10])
            new_end = int(wds[8]) - int(wds[11]) + 1

        # I want a tuple of (score, old_chr, old_start, old_end,
        # new_chr, new_start, new_end, is_forward)
        return (
            int(wds[1]),
            self._loki.chr_num[wds[2][3:]],
            int(wds[5]) + 1,
            int(wds[6]),
            self._loki.chr_num.get(wds[7][3:], -1),
            new_start,
            new_end,
            int(is_fwd),
        )

    def _parseData(self, chain_tuple, chain_data):
        """
        Parses the chain data into a more readily usable and iterable
        form (the data of the chain is everything after the 1st line)
        """
        _data = [
            tuple([int(v) for v in ln.split()])
            for ln in chain_data.split("\n")[:-1]  # noqa: E501
        ]

        curr_pos = chain_tuple[2]
        new_pos = chain_tuple[5]

        _data_txform = []
        for ln in _data:
            _data_txform.append((curr_pos, curr_pos + ln[0] - 1, new_pos))
            curr_pos = curr_pos + ln[0] + ln[1]
            if chain_tuple[7]:
                new_pos = new_pos + ln[0] + ln[2]
            else:
                new_pos = new_pos - ln[0] - ln[2]

        _data_txform.append(
            (curr_pos, curr_pos + int(chain_data.split()[-1]) - 1, new_pos)
        )

        return _data_txform
