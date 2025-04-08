#!/usr/bin/env python
import sys
import collections
import os
import re
import urllib.request as urllib2
from loki_modules import loki_source


class Source_dbsnp(loki_source.Source):

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
        "PAR",
        "MT",
    )

    ##################################################
    # private class data

    def _identifyLatestSNPContig(self, filenames):
        # 		reFile = re.compile(r'^b([0-9]+)_SNPContigLocusId(.*)\.bcp\.gz$', re.IGNORECASE)
        bestbuild = 0
        bestfile = list()
        for filename in filenames:
            # 			match = reFile.match(filename)
            # 			if match:
            # 				filebuild = int(match.group(1))
            # 				if filebuild > bestbuild:
            # 					bestbuild = filebuild
            # 					bestfile = filename
            # foreach file in path
            if int(filename[0]) > bestbuild:
                bestfile.append(filename[0])
                bestfile.append(filename[1].split(".bcp.gz")[0])
                bestbuild = int(filename[0])
        return bestfile

    # _identifyLatestSNPContig()

    ##################################################
    # source interface

    @classmethod
    def getVersionString(cls):
        return "2.3 (2018-11-01)"

    # getVersionString()

    @classmethod
    def getOptions(cls):
        return {
            "unvalidated": "[yes|no]  --  store SNP loci which have not been validated (default: yes)",
            "suspect": "[yes|no]  --  store SNP loci which are suspect (default: no)",  # http://www.ncbi.nlm.nih.gov/projects/SNP/docs/rs_attributes.html#suspect
            "withdrawn": "[yes|no]  --  store SNP loci which have been withdrawn (default: no)",
            "loci": "[all|validated]  --  store all or only validated SNP loci (default: validat`dddded)",
            "merges": "[yes|no]  --  process and store RS# merge history (default: yes)",
            "roles": "[yes|no]  --  process and store SNP roles (default: no)",
        }

    # getOptions()

    def validateOptions(self, options):
        options.setdefault("unvalidated", "yes")
        options.setdefault("suspect", "no")
        options.setdefault("withdrawn", "no")
        options.setdefault("merges", "yes")
        options.setdefault("roles", "no")
        for o, v in options.items():
            v = v.strip().lower()
            if o in ("unvalidated", "suspect", "withdrawn", "merges", "roles"):
                if "yes".startswith(v):
                    v = "yes"
                elif "no".startswith(v):
                    v = "no"
                else:
                    return "%s must be 'yes' or 'no'" % o
            else:
                return "unknown option '%s'" % o
            options[o] = v
        return True

    # validateOptions()

    def download(self, options):
        # define a callback to identify the latest SNPContigLocusId file
        def remFilesCallback(ftp):
            remFiles = dict()
            for chm in self._chmList:
                remFiles["chr_%s.txt.gz" % chm] = (
                    "/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz" % chm
                )

            if options["merges"] == "yes":
                remFiles["RsMergeArch.bcp.gz"] = (
                    "/snp/organisms/human_9606/database/organism_data/RsMergeArch.bcp.gz"
                )

            if options.get["roles"] == yes:
                remFiles["SnpFunctionCode.bcp.gz"] = (
                    "/snp/organisms/database/shared_data/SnpFunctionCode.bcp.gz"
                )
                path = "/snp/organisms/human_9606/database/organism_data"
                ftp.cwd(path)
                bestfile = self._identifyLatestSNPContig(ftp.nlst())

                if bestfile:
                    remFiles[bestfile] = "%s/%s" % (path, bestfile)

            return remFiles

        # remFilesCallback

        remFiles = dict()
        for chm in self._chmList:
            remFiles["chr_%s.txt.gz" % chm] = (
                "/snp/organisms/human_9606/chr_rpts/chr_%s.txt.gz" % chm
            )
        if options["merges"] == "yes":
            remFiles["RsMergeArch.bcp.gz"] = (
                "/snp/organisms/human_9606/database/organism_data/RsMergeArch.bcp.gz"
            )
        if options["roles"] == "yes":
            remFiles["SnpFunctionCode.bcp.gz"] = (
                "/snp/organisms/database/shared_data/SnpFunctionCode.bcp.gz"
            )
            path = "/snp/organisms/human_9606/database/organism_data"
            urlpath = urllib2.urlopen("https://ftp.ncbi.nih.gov" + path)
            string = urlpath.read().decode("utf-8")
            onlyfiles = list(
                set(re.findall(r"b([0-9]+)_SNPContigLocusId_(.*)\.bcp\.gz", string))
            )
            bestfile = self._identifyLatestSNPContig(onlyfiles)
            bestfilename = (
                "b" + bestfile[0] + "_SNPContigLocusId_" + bestfile[1] + ".bcp.gz"
            )
            if bestfile:
                remFiles[bestfilename] = "%s/%s" % (path, bestfilename)

        # download the latest source files
        # 		self.downloadFilesFromFTP('ftp.ncbi.nih.gov', remFilesCallback)
        self.downloadFilesFromHTTP("ftp.ncbi.nih.gov", remFiles)

    # download()

    def update(self, options):
        # clear out all old data from this source
        self.log("deleting old records from the database ...")
        self.deleteAll()
        self.log(" OK\n")

        # process merge report (no header!)
        if options.get("merges", "yes") == "yes":
            """/* from human_9606_table.sql.gz */
            CREATE TABLE [RsMergeArch]
            (
            [rsHigh] [int] NULL ,
            [rsLow] [int] NULL ,
            [build_id] [int] NULL ,
            [orien] [tinyint] NULL ,
            [create_time] [datetime] NOT NULL ,
            [last_updated_time] [datetime] NOT NULL ,
            [rsCurrent] [int] NULL ,
            [orien2Current] [tinyint] NULL ,
            [comment] [varchar](255) NULL
            )
            """
            self.log("processing SNP merge records ...")
            mergeFile = self.zfile(
                "RsMergeArch.bcp.gz"
            )  # TODO:context manager,iterator
            numMerge = 0
            setMerge = set()
            for line in mergeFile:
                words = line.split("\t")
                if not (len(words) > 6 and words[0] and words[6]):
                    continue
                rsOld = int(words[0])
                # rsNew = int(words[1])
                rsCur = int(words[6])

                setMerge.add((rsOld, rsCur))

                # write to the database after each 2.5 million, to keep memory usage down
                if len(setMerge) >= 2500000:
                    numMerge += len(setMerge)
                    self.log(
                        " ~%1.1f million so far\n" % (numMerge / 1000000.0)
                    )  # TODO: time estimate
                    self.log("writing SNP merge records to the database ...")
                    self.addSNPMerges(setMerge)
                    setMerge = set()
                    self.log(" OK\n")
                    self.log("processing SNP merge records ...")
            # foreach line in mergeFile
            numMerge += len(setMerge)
            self.log(" OK: ~%d merged RS#s\n" % numMerge)
            if setMerge:
                self.log("writing SNP merge records to the database ...")
                self.addSNPMerges(setMerge)
                self.log(" OK\n")
            setMerge = None
        # if merges

        # process SNP role function codes
        if options.get("roles", "no") == "yes":
            """/* from dbSNP_main_table.sql.gz */
            CREATE TABLE [SnpFunctionCode]
            (
            [code] [tinyint] NOT NULL ,
            [abbrev] [varchar](20) NOT NULL ,
            [descrip] [varchar](255) NOT NULL ,
            [create_time] [smalldatetime] NOT NULL ,
            [top_level_class] [char](5) NOT NULL ,
            [is_coding] [tinyint] NOT NULL ,
            [is_exon] [bit] NULL ,
            [var_prop_effect_code] [int] NULL ,
            [var_prop_gene_loc_code] [int] NULL ,
            [SO_id] [varchar](32) NULL
            )
            """
            self.log("processing SNP role codes ...")
            roleID = dict()
            codeFile = self.zfile("SnpFunctionCode.bcp.gz")
            for line in codeFile:
                words = line.split("\t")
                code = int(words[0])
                name = words[1]
                desc = words[2]
                coding = int(words[5]) if (len(words) > 5 and words[5] != "") else None
                exon = int(words[6]) if (len(words) > 6 and words[6] != "") else None

                roleID[code] = self.addRole(name, desc, coding, exon)
            # foreach line in codeFile
            self.log(" OK: %d codes\n" % len(roleID))

            # process SNP roles
            """ /* from human_9606_table.sql.gz */
CREATE TABLE [b137_SNPContigLocusId]
(
[snp_id] [int] NULL ,
[contig_acc] [varchar](32) NOT NULL ,
[contig_ver] [tinyint] NULL ,
[asn_from] [int] NULL ,
[asn_to] [int] NULL ,
[locus_id] [int] NULL ,
[locus_symbol] [varchar](64) NULL ,
[mrna_acc] [varchar](32) NOT NULL ,
[mrna_ver] [smallint] NOT NULL ,
[protein_acc] [varchar](32) NULL ,
[protein_ver] [smallint] NULL ,
[fxn_class] [int] NULL ,
[reading_frame] [int] NULL ,
[allele] [varchar](255) NULL ,
[residue] [varchar](1000) NULL ,
[aa_position] [int] NULL ,
[build_id] [varchar](4) NOT NULL ,
[ctg_id] [int] NULL ,
[mrna_start] [int] NULL ,
[mrna_stop] [int] NULL ,
[codon] [varchar](1000) NULL ,
[protRes] [char](3) NULL ,
[contig_gi] [int] NULL ,
[mrna_gi] [int] NULL ,
[mrna_orien] [tinyint] NULL ,
[cp_mrna_ver] [int] NULL ,
[cp_mrna_gi] [int] NULL ,
[verComp] [int] NULL
)
"""
            self.log("processing SNP roles ...")
            setRole = set()
            numRole = numOrphan = numInc = 0
            setOrphan = set()
            funcFile = self.zfile(self._identifyLatestSNPContig(os.listdir(".")))
            for line in funcFile:
                words = list(w.strip() for w in line.split("\t"))
                rs = int(words[0]) if words[0] else None
                entrez = int(words[5]) if words[5] else None
                # genesymbol = words[6]
                code = int(words[11]) if words[11] else None

                if rs and entrez and code:
                    try:
                        setRole.add((rs, entrez, roleID[code]))
                    except KeyError:
                        setOrphan.add(code)
                        numOrphan += 1
                else:
                    numInc += 1

                # write to the database after each 2.5 million, to keep memory usage down
                if len(setRole) >= 2500000:
                    numRole += len(setRole)
                    self.log(
                        " ~%1.1f million so far\n" % (numRole / 1000000.0)
                    )  # TODO: time estimate
                    self.log("writing SNP roles to the database ...")
                    self.addSNPEntrezRoles(setRole)
                    setRole = set()
                    self.log(" OK\n")
                    self.log("processing SNP roles ...")

            roleID = None
            # foreach line in funcFile
            numRole += len(setRole)
            self.log(" OK: ~%d roles\n" % (numRole,))
            if setRole:
                self.log("writing SNP roles to the database ...")
                self.addSNPEntrezRoles(setRole)
                self.log(" OK\n")
            setRole = None

            # warn about orphans
            self.logPush()
            if setOrphan:
                self.log(
                    "WARNING: %d roles (%d codes) unrecognized\n"
                    % (numOrphan, len(setOrphan))
                )
            if numInc:
                self.log("WARNING: %d roles incomplete\n" % (numInc,))
            setOrphan = None
            self.logPop()
        # if roles

        # process chromosome report files
        # dbSNP chromosome reports use 1-based coordinates since b125, according to:
        #   http://www.ncbi.nlm.nih.gov/books/NBK44414/#Reports.the_xml_dump_for_build_126_has_a
        # This matches LOKI's convention.
        grcBuild = None
        reBuild = re.compile("GRCh([0-9]+)")
        includeUnvalidated = options["unvalidated"] == "yes"
        includeSuspect = options["suspect"] == "yes"
        includeWithdrawn = options["withdrawn"] == "yes"
        for fileChm in self._chmList:
            self.log("processing chromosome %s SNPs ..." % fileChm)
            chmFile = self.zfile("chr_%s.txt.gz" % fileChm)

            # verify file headers
            header1 = chmFile.__next__().rstrip()
            chmFile.__next__()
            chmFile.__next__()
            header2 = chmFile.__next__().rstrip()
            header3 = chmFile.__next__().rstrip()
            chmFile.__next__()
            chmFile.__next__()
            if not header1.startswith("dbSNP Chromosome Report"):
                raise Exception("ERROR: unrecognized file header '%s'" % header1)
            if not header2.startswith(
                "rs#\tmap\tsnp\tchr\tctg\ttotal\tchr\tctg\tctg\tctg\tctg\tchr\tlocal\tavg\ts.e.\tmax\tvali-\tgeno-\tlink\torig\tupd"
            ):
                raise Exception("ERROR: unrecognized file subheader '%s'" % header2)
            if not header3.startswith(
                "\twgt\ttype\thits\thits\thits\t\tacc\tver\tID\tpos\tpos\tloci\thet\thet\tprob\tdated\ttypes\touts\tbuild\tbuild"
            ):
                raise Exception("ERROR: unrecognized file subheader '%s'" % header3)

            # process lines
            numPos = numPosBatch = 0
            listChrPos = collections.defaultdict(list)
            setBadBuild = set()
            setBadVers = set()
            setBadFilter = set()
            setBadChr = set()
            for line in chmFile:
                words = line.split("\t")
                rs = words[0].strip()
                withdrawn = int(words[2].strip()) > 0
                chm = words[6].strip()
                pos = words[11].strip()
                validated = 1 if (int(words[16].strip()) > 0) else 0
                build = reBuild.search(words[21])
                suspect = int(words[22].strip()) > 0

                if rs != "" and chm != "" and pos != "":
                    rs = int(rs)
                    pos = int(pos)
                    if not build:
                        setBadBuild.add(rs)
                    elif grcBuild and grcBuild != build.group(1):
                        setBadVers.add(rs)
                    elif not (validated or includeUnvalidated):
                        setBadFilter.add(rs)
                    elif suspect and not includeSuspect:
                        setBadFilter.add(rs)
                    elif withdrawn and not includeWithdrawn:
                        setBadFilter.add(rs)
                    elif (fileChm != "PAR") and (chm != fileChm):
                        setBadChr.add(rs)
                    elif (fileChm == "PAR") and (chm != "X") and (chm != "Y"):
                        setBadChr.add(rs)
                    else:
                        if not grcBuild:
                            grcBuild = build.group(1)
                        numPosBatch += 1
                        listChrPos[chm].append((rs, pos, validated))
                        setBadChr.discard(rs)
                        setBadFilter.discard(rs)
                        setBadVers.discard(rs)
                        setBadBuild.discard(rs)
                        if numPosBatch >= 2500000:
                            numPos += numPosBatch
                            numPosBatch = 0
                            self.log(" %1.1f million so far\n" % (numPos / 1000000.0))
                            # store data
                            self.log(
                                "writing chromosome %s SNPs to the database ..."
                                % fileChm
                            )
                            for chm, listPos in listChrPos.items():
                                self.addChromosomeSNPLoci(
                                    self._loki.chr_num[chm], listPos
                                )
                            listChrPos = collections.defaultdict(list)
                            self.log(" OK\n")
                            self.log("processing chromosome %s SNPs ..." % fileChm)
                    # if rs/chm/pos provided
            # foreach line in chmFile
            self.log(" OK: %d SNP loci\n" % (numPos,))
            # store data
            if listChrPos:
                self.log("writing chromosome %s SNPs to the database ..." % fileChm)
                for chm, listPos in listChrPos.items():
                    self.addChromosomeSNPLoci(self._loki.chr_num[chm], listPos)
                self.log(" OK\n")

            # print results
            numPos += numPosBatch
            setBadFilter.difference_update(setBadChr)
            setBadVers.difference_update(setBadChr, setBadFilter)
            setBadBuild.difference_update(setBadChr, setBadFilter, setBadVers)
            self.logPush()
            if setBadBuild:
                self.log(
                    "WARNING: %d SNPs not mapped to any GRCh build\n"
                    % (len(setBadBuild))
                )
            if setBadVers:
                self.log(
                    "WARNING: %d SNPs mapped to GRCh build version other than %s\n"
                    % (len(setBadVers), grcBuild)
                )
            if setBadFilter:
                self.log(
                    "WARNING: %d SNPs skipped (unvalidated, suspect and/or withdrawn)\n"
                    % (len(setBadFilter))
                )
            if setBadChr:
                self.log(
                    "WARNING: %d SNPs on mismatching chromosome\n" % (len(setBadChr))
                )
            self.logPop()
            listChrPos = setBadBuild = setBadVers = setBadFilter = setBadChr = None
        # foreach chromosome

        # store source metadata
        self.setSourceBuilds(grcBuild, None)

    # update()


# Source_dbsnp
